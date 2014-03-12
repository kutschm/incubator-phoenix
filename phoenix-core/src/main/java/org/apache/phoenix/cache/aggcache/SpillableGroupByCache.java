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
package org.apache.phoenix.cache.aggcache;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.coprocessor.GroupByCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

/**
 * The main entry point is in GroupedAggregateRegionObserver. It instantiates a SpillableGroupByCache and invokes a
 * get() method on it. There is no: "if key not exists -> put into map" case, since the cache is a Loading cache and
 * therefore handles the put under the covers. I tried to implement the final cache element accesses (RegionScanner
 * below) streaming, i.e. there is just an iterator on it and removed the existing result materialization.
 * SpillableGroupByCache implements a LRU cache using a LinkedHashMap with access order. There is a configurable an
 * upper and lower size limit in bytes which are used as follows to compute the initial cache size in number of
 * elements: Max(lowerBoundElements, Min(upperBoundElements, estimatedCacheSize)). Once the number of cached elements
 * exceeds this number, the cache size is increased by a factor of 1.5. This happens until the additional memory to grow
 * the cache cannot be requested. At this point the Cache starts spilling elements. As long as no eviction happens no
 * spillable data structures are allocated, this only happens as soon as the first element is evicted from the cache. We
 * cannot really make any assumptions on which keys arrive at the map, but assume the LRU would at least cover the cases
 * where some keys have a slight skew and they should stay memory resident. Once a key gets evicted, the spillManager is
 * instantiated. It basically takes care of spilling an element to disk and does all the SERDE work. It pre-allocates a
 * configurable number of SpillFiles (spill partition) which are memory mapped temp files. The SpillManager keeps a list
 * of these and hash distributes the keys within this list. Once an element gets spilled, it is serialized and will only
 * get deserialized again, when it is requested from the client, i.e. loaded back into the LRU cache. The SpillManager
 * holds a single SpillMap object in memory for every spill partition (SpillFile). The SpillMap is an in memory Map
 * representation of a single page of spilled serialized key/value pairs. To achieve fast key lookup the key is hash
 * partitioned into random pages of the current spill file. The code implements an extendible hashing approach which
 * dynamically adjusts the hash function, in order to adapt to growing number of storage pages and avoiding long chains
 * of overflow buckets. For an excellent discussion of the algorithm please refer to the following online resource:
 * http://db.inf.uni-tuebingen.de/files/teaching/ws1011/db2/db2-hash-indexes.pdf . For this, each SpillFile keeps a
 * directory of pointers to Integer.MAX_VALUE 4K pages in memory, which allows each directory to address more pages than
 * a single memory mapped temp file could theoretically store. In case directory doubling, requests a page index that
 * exceeds the limits of the initial temp file limits, the implementation dynamically allocates additional temp files to
 * the SpillFile. The directory starts with a global depth of 1 and therefore a directory size of 2 buckets. Only during
 * bucket split and directory doubling more than one page is temporarily kept in memory until all elements have been
 * redistributed. The current implementation conducts bucket splits as long as an element does not fit onto a page. No
 * overflow chain is created, which might be an alternative. For get requests, each directory entry maintains a
 * bloomFilter to prevent page-in operations in case an element has never been spilled before. The deserialization is
 * only triggered when a key a loaded back into the LRU cache. The aggregators are returned from the LRU cache and the
 * next value is computed. In case the key is not found on any page, the Loader create new aggregators for it.
 */

public class SpillableGroupByCache extends GroupByCache {

    private static final Logger logger = LoggerFactory.getLogger(SpillableGroupByCache.class);

    /*
     * inner class that makes cache queryable for other classes that should not get the full instance. Queryable view of
     * the cache
     */
    public class QueryCache {
        public boolean isKeyContained(ImmutableBytesPtr key) {
            return cache.containsKey(key);
        }
    }

    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples used for group by queries
     * 
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public SpillableGroupByCache(final RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
            ServerAggregators aggs, final int estSizeNum, List<Expression> expressions) {
        super(env, tenantId, aggs, estSizeNum, expressions);

        final Configuration conf = env.getConfiguration();
        final int numSpillFilesConf = conf.getInt(QueryServices.GROUPBY_SPILL_FILES_ATTRIB,
                QueryServicesOptions.DEFAULT_GROUPBY_SPILL_FILES);

        // TODO lazy instantiation
        spillManager = new SpillManager(numSpillFilesConf, aggregators, env.getConfiguration(), new QueryCache());
        initCache();
    }

    /**
     * Extract an element from the Cache If element is not present in in-memory cache / or in spill files cache
     * implements an implicit put() of a new key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] cache(ImmutableBytesWritable cacheKey) {
        ImmutableBytesPtr key = new ImmutableBytesPtr(cacheKey);
        Aggregator[] rowAggregators = cache.get(key);
        if (rowAggregators == null) {
            // If Aggregators not found for this distinct
            // value, clone our original one (we need one
            // per distinct value)
            if (spillManager.initialized()) {
                // Spill manager present, check if key has been
                // spilled before
                try {
                    rowAggregators = spillManager.loadEntry(key);
                } catch (IOException ioe) {
                    // Ensure that we always close and delete the temp files
                    try {
                        throw new RuntimeException(ioe);
                    } finally {
                        Closeables.closeQuietly(SpillableGroupByCache.this);
                    }
                }
            }
            if (rowAggregators == null) {
                // No, key never spilled before, create a new tuple
                rowAggregators = aggregators.newAggregators(env.getConfiguration());
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new aggregate bucket for row key "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()));
                }
            }
            cache.put(key, rowAggregators);
            // keep track of elements in cache
            curNumCacheElements++;
        }
        return rowAggregators;
    }

    /**
     * Iterator over the cache and the spilled data structures by returning CacheEntries. CacheEntries are either
     * extracted from the LRU cache or from the spillable data structures.The key/value tuples are returned in
     * non-deterministic order.
     */
    private final class EntryIterator implements Iterator<Tuple> {
        final Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> cacheIter;
        final Iterator<Tuple> spilledCacheIter;

        private EntryIterator() {
            cacheIter = cache.entrySet().iterator();
            if (spillManager != null) {
                spilledCacheIter = spillManager.newDataIterator();
            } else {
                spilledCacheIter = null;
            }
        }

        @Override
        public boolean hasNext() {
            return cacheIter.hasNext();
        }

        @Override
        public Tuple next() {
            if (spilledCacheIter != null && spilledCacheIter.hasNext()) {
                Tuple spilledTuple = spilledCacheIter.next();
                // Deserialize into a CacheEntry
                boolean notFound = false;
                // check against map and return only if not present
                ImmutableBytesPtr key = new ImmutableBytesPtr();
                spilledTuple.getKey(key);
                while (cache.containsKey(key)) {
                    // LRU Cache entries always take precedence,
                    // since they are more up to date
                    if (spilledCacheIter.hasNext()) {
                        logger.error("spillNext");
                        spilledTuple = spilledCacheIter.next();
                        spilledTuple.getKey(key);
                        // spilledEntry = spillManager.toCacheEntry(value);
                    } else {
                        notFound = true;
                        break;
                    }
                }
                if (!notFound) {
                    // Return a spilled entry, this only happens if the
                    // entry was not
                    // found in the LRU cache
                    return spilledTuple;
                }
            }
            // Spilled elements exhausted
            // Finally return all elements from LRU cache
            // return cacheIter.next();
            Map.Entry<ImmutableBytesWritable, Aggregator[]> ce = cacheIter.next();
            ImmutableBytesWritable key = ce.getKey();
            Aggregator[] aggs = ce.getValue();
            byte[] value = aggregators.toBytes(aggs);
            if (logger.isDebugEnabled()) {
                logger.debug("Adding new distinct group: "
                        + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()) + " with aggregators "
                        + aggs.toString() + " value = " + Bytes.toStringBinary(value));
            }
            return new SingleKeyValueTuple(KeyValueUtil.newKeyValue(key.get(), key.getOffset(), key.getLength(),
                    QueryConstants.SINGLE_COLUMN_FAMILY, QueryConstants.SINGLE_COLUMN, QueryConstants.AGG_TIMESTAMP,
                    value, 0, value.length));
        }

        /**
         * Remove??? Denied!!!
         */
        @Override
        public void remove() {
            throw new IllegalAccessError("Remove is not supported for this type of iterator");
        }
    }

    /**
     * Closes cache and releases spill resources
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);
        Closeables.closeQuietly(chunk);
    }

    @Override
    public RegionScanner getScanner(final RegionScanner s) {
        final Iterator<Tuple> cacheIter = new EntryIterator();

        // scanner using the spillable implementation
        return new BaseRegionScanner() {
            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public void close() throws IOException {
                try {
                    s.close();
                } finally {
                    // Always close gbCache and swallow possible Exceptions
                    Closeables.closeQuietly(SpillableGroupByCache.this);
                }
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                if (!cacheIter.hasNext()) { return false; }
                results.add(cacheIter.next().getValue(0));
                return cacheIter.hasNext();
            }

            @Override
            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }
        };
    }
}