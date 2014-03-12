/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILLABLE_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_SPILLABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.cache.aggcache.HBaseSpillableGroupBy;
import org.apache.phoenix.cache.aggcache.SpillableGroupByCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.KeyValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class GroupByCacheFactory {

    private static final Logger logger = LoggerFactory.getLogger(GroupByCacheFactory.class);

    static GroupByCache newCache(RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
            ServerAggregators aggregators, int estDistVals, final List<Expression> expressions) {

        Configuration conf = env.getConfiguration();
        final int cacheType = conf.getInt(GROUPBY_SPILLABLE_ATTRIB, DEFAULT_GROUPBY_SPILLABLE);

        if (logger.isDebugEnabled()) {
            logger.debug("Spillable groupby enabled: "
                    + Boolean.valueOf(cacheType != QueryServicesOptions.GROUPBY_INMEMORY));
        }

        switch (cacheType) {
        case QueryServicesOptions.GROUPBY_SPILLABLE:
            if (logger.isDebugEnabled()) {
                logger.debug("Using spillable cache");
            }
            return new SpillableGroupByCache(env, tenantId, aggregators, estDistVals, expressions);
        case QueryServicesOptions.GROUPBY_HBASE:
            if (logger.isDebugEnabled()) {
                logger.debug("Using hbase cache");
            }
            return new HBaseSpillableGroupBy(env, tenantId, aggregators, estDistVals, expressions);
        case QueryServicesOptions.GROUPBY_INMEMORY:
        default:
            if (logger.isDebugEnabled()) {
                logger.debug("Using in-memory cache");
            }
            return new InMemoryGroupByCache(env, tenantId, aggregators, estDistVals, expressions);
        }
    }

    /**
     * Cache for distinct values and their aggregations which is completely in-memory (as opposed to spilling to disk).
     * Used when GROUPBY_SPILLABLE_ATTRIB is set to false. The memory usage is tracked at a coursed grain and will throw
     * and abort if too much is used.
     * 
     * @author jtaylor
     * @since 3.0.0
     */
    private static final class InMemoryGroupByCache extends GroupByCache {
        private final MemoryChunk chunk;
        private final Map<ImmutableBytesPtr, Aggregator[]> aggregateMap;
        private final ServerAggregators aggregators;
        private final RegionCoprocessorEnvironment env;

        private int estDistVals;

        InMemoryGroupByCache(RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
                ServerAggregators aggregators, int estDistVals, final List<Expression> expressions) {

            super(env, tenantId, aggregators, estDistVals, expressions);

            int estValueSize = aggregators.getEstimatedByteSize();
            int estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(estDistVals, estValueSize);
            TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);
            this.env = env;
            this.estDistVals = estDistVals;
            this.aggregators = aggregators;
            this.aggregateMap = Maps.newHashMapWithExpectedSize(estDistVals);
            this.chunk = tenantCache.getMemoryManager().allocate(estSize);
        }

        @Override
        public void close() throws IOException {
            this.chunk.close();
        }

        @Override
        public Aggregator[] cache(ImmutableBytesWritable cacheKey) {
            ImmutableBytesPtr key = new ImmutableBytesPtr(cacheKey);
            Aggregator[] rowAggregators = aggregateMap.get(key);
            if (rowAggregators == null) {
                // If Aggregators not found for this distinct
                // value, clone our original one (we need one
                // per distinct value)
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new aggregate bucket for row key "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()));
                }
                rowAggregators = aggregators.newAggregators(env.getConfiguration());
                aggregateMap.put(key, rowAggregators);

                if (aggregateMap.size() > estDistVals) { // increase allocation
                    estDistVals *= 1.5f;
                    int estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(estDistVals,
                            aggregators.getEstimatedByteSize());
                    chunk.resize(estSize);
                }
            }
            return rowAggregators;
        }

        @Override
        public RegionScanner getScanner(final RegionScanner s) {
            // Compute final allocation
            int estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(aggregateMap.size(),
                    aggregators.getEstimatedByteSize());
            chunk.resize(estSize);

            final List<KeyValue> aggResults = new ArrayList<KeyValue>(aggregateMap.size());

            final Iterator<Map.Entry<ImmutableBytesPtr, Aggregator[]>> cacheIter = aggregateMap.entrySet().iterator();
            while (cacheIter.hasNext()) {
                Map.Entry<ImmutableBytesPtr, Aggregator[]> entry = cacheIter.next();
                ImmutableBytesPtr key = entry.getKey();
                Aggregator[] rowAggregators = entry.getValue();
                // Generate byte array of Aggregators and set as value of row
                byte[] value = aggregators.toBytes(rowAggregators);

                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new distinct group: "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()) + " with aggregators "
                            + Arrays.asList(rowAggregators).toString() + " value = " + Bytes.toStringBinary(value));
                }
                KeyValue keyValue = KeyValueUtil.newKeyValue(key.get(), key.getOffset(), key.getLength(),
                        QueryConstants.SINGLE_COLUMN_FAMILY, QueryConstants.SINGLE_COLUMN,
                        QueryConstants.AGG_TIMESTAMP, value, 0, value.length);
                aggResults.add(keyValue);
            }
            // scanner using the non spillable, memory-only implementation
            return new BaseRegionScanner() {
                private int index = 0;

                @Override
                public HRegionInfo getRegionInfo() {
                    return s.getRegionInfo();
                }

                @Override
                public void close() throws IOException {
                    try {
                        s.close();
                    } finally {
                        InMemoryGroupByCache.this.close();
                    }
                }

                @Override
                public boolean next(List<Cell> results) throws IOException {
                    if (index >= aggResults.size()) return false;
                    results.add(aggResults.get(index));
                    index++;
                    return index < aggResults.size();
                }

                @Override
                public long getMaxResultSize() {
                    // TODO Auto-generated method stub
                    return 0;
                }
            };
        }

        @Override
        public int size() {
            return aggregateMap.size();
        }
    }
}
