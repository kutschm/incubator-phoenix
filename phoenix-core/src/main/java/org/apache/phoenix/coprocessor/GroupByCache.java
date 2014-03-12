/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.cache.aggcache.ISpillManager;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.memory.InsufficientMemoryException;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

/**
 * Interface to abstract the way in which distinct group by elements are cached
 * This implements an in-memory LRU cache that grows by a factor of 1.5 in case it capacity is exhausted. 
 * Once the the in-memory structure cannot grow any further, the cache starts evicting elements, i.e. it starts spilling.
 * 
 * Subclasses are supposed to implement a spill strategy for the evicted LRU elements.
 * 
 * @since 3.0.0
 */
public abstract class GroupByCache implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(GroupByCache.class);

    // Min size of 1st level main memory cache in bytes --> lower bound
    private static final int SPGBY_CACHE_MIN_SIZE = 4096; // 4K

    // TODO Generally better to use Collection API with generics instead of
    // array types
    protected LinkedHashMap<ImmutableBytesWritable, Aggregator[]> cache;
    protected ISpillManager<Tuple> spillManager = null;
    protected int curNumCacheElements;
    protected final ServerAggregators aggregators;
    protected final RegionCoprocessorEnvironment env;
    protected final MemoryChunk chunk;
    protected final List<Expression> expressions;

    private int maxCacheSize = 0;
    private int estValueSize = 0;

    public GroupByCache(final RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
            ServerAggregators aggs, final int estSizeNum, List<Expression> expressions) {
        curNumCacheElements = 0;
        this.aggregators = aggs;
        this.env = env;
        this.expressions = expressions;

        estValueSize = aggregators.getEstimatedByteSize();
        final TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);

        // Compute Map initial map
        final Configuration conf = env.getConfiguration();
        final long maxCacheSizeConf = conf.getLong(QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_GROUPBY_MAX_CACHE_MAX);

        final int maxSizeNum = (int)(maxCacheSizeConf / estValueSize);
        final int minSizeNum = (SPGBY_CACHE_MIN_SIZE / estValueSize);

        // use upper and lower bounds for the cache size
        maxCacheSize = Math.max(minSizeNum, Math.min(maxSizeNum, estSizeNum));
        final int estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(maxCacheSize, estValueSize);
        try {
            this.chunk = tenantCache.getMemoryManager().allocate(estSize);
        } catch (InsufficientMemoryException ime) {
            logger.error("Requested Map size exceeds memory limit, please decrease max size via config paramter: "
                    + QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB);
            throw ime;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating LRU groupby cache of element size: " + maxCacheSize);
        }
    }

    protected void initCache() {
        // LRU cache implemented as LinkedHashMap with access order
        cache = new LinkedHashMap<ImmutableBytesWritable, Aggregator[]>(1, 0.75f, true) {            

            @Override
            protected boolean removeEldestEntry(Map.Entry<ImmutableBytesWritable, Aggregator[]> eldest) {
                if ((!spillManager.initialized()) && size() > maxCacheSize) { // increase allocation
                    maxCacheSize *= 1.5f;
                    int estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(maxCacheSize, estValueSize);
                    try {
                        chunk.resize(estSize);
                    } catch (InsufficientMemoryException im) {
                        // Cannot extend Map anymore, start spilling
                        spillManager.init();
                    }
                }

                if (spillManager.initialized()) {
                    try {
                        spillManager.spill(eldest.getKey(), eldest.getValue());
                        // keep track of elements in cache
                        curNumCacheElements--;
                    } catch (IOException ioe) {
                        // Ensure that we always close and delete the temp files
                        try {
                            throw new RuntimeException(ioe);
                        } finally {
                            Closeables.closeQuietly(GroupByCache.this);
                        }
                    }
                    return true;
                }

                return false;
            }
        };
    }

    /**
     * Size function returns the estimate LRU cache size in bytes
     */
    public int size() {
        return curNumCacheElements * aggregators.getEstimatedByteSize();
    }

    /**
     * Function to add elements to the cache
     */
    public abstract Aggregator[] cache(ImmutableBytesWritable key);

    /**
     * Function to return a scanner over all in-memory and spilled elements.
     */
    public abstract RegionScanner getScanner(RegionScanner s);
}
