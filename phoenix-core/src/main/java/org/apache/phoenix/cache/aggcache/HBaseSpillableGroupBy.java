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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.coprocessor.GroupByCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;

import com.google.common.io.Closeables;

/**
 * Spillable GroupBy implementation using an HBase table to spill. This groupby implementation uses an in-memory LRU
 * cache which dynamically grows until memory is exhausted. At this point the cache starts spilling evicted elements
 * into an HBase table. Keys are never updated, instead multiple spills on the same key are store with multiple row
 * versions in HBase. The final aggregate is computed by iterating over all versions for a single key and merging the
 * partial aggregates together.
 */
public class HBaseSpillableGroupBy extends GroupByCache {

    private final Configuration conf;
    // private final HTableInterface htable;
    private final ServerAggregators aggregators;

    public HBaseSpillableGroupBy(final RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
            ServerAggregators aggs, int estSizeNum, final List<Expression> expressions) {

        super(env, tenantId, aggs, estSizeNum, expressions);
        this.conf = env.getConfiguration();
        this.aggregators = aggs;

        spillManager = new HBaseSpillManager(env, aggs, expressions);
        initCache();
    }

    @Override
    public Aggregator[] cache(ImmutableBytesWritable cacheKey) {
        ImmutableBytesPtr key = new ImmutableBytesPtr(cacheKey);
        Aggregator[] rowAggregators = cache.get(key);
        if (rowAggregators == null) {
            // Key not contained in in-memory cache, start a new partial
            rowAggregators = aggregators.newAggregators(conf);
            cache.put(key, rowAggregators);
        }
        return rowAggregators;
    }

    @Override
    public RegionScanner getScanner(final RegionScanner s) {
        ISpillManager<Tuple> spillManager = super.spillManager;
        Iterator<Tuple> iter = null;
        if (spillManager.initialized()) {
            iter = spillManager.newDataIterator();
        }
        final Iterator<Tuple> scannerIter = iter;

        return new BaseRegionScanner() {
            private ImmutableBytesWritable currentKey = null;
            Iterator<Entry<ImmutableBytesWritable, Aggregator[]>> cacheIter = null;

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
                    Closeables.closeQuietly(HBaseSpillableGroupBy.this);
                }
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                boolean aggBoundary = false;
                ImmutableBytesWritable key = new ImmutableBytesWritable();
                Tuple tuple = new MultiKeyValueTuple();
                Aggregator[] rowAggregators = aggregators.getAggregators();
                if (cache.containsKey(key)) {
                    // key contained in in-memory cache, use its rowAggregators first
                    rowAggregators = cache.get(key);
                }
                if (scannerIter != null) {
                    while (scannerIter.hasNext() && !aggBoundary) {
                        tuple = scannerIter.next();
                        // Results are potentially returned even when the return
                        // value of s.next is false
                        // since this is an indication of whether or not there
                        // are more values after the
                        // ones returned
                        tuple.getKey(key);
                        // key = TupleUtil.getConcatenatedValue(tuple, expressions);
                        aggBoundary = ((currentKey != null) && (currentKey.compareTo(key) != 0));
                        if (!aggBoundary) {
                            aggregators.aggregate(rowAggregators, tuple);
                            currentKey = key;
                        }
                    }

                    if (currentKey != null) {
                        byte[] value = aggregators.toBytes(rowAggregators);
                        KeyValue keyValue = KeyValueUtil.newKeyValue(currentKey.get(), currentKey.getOffset(),
                                currentKey.getLength(), QueryConstants.SINGLE_COLUMN_FAMILY,
                                QueryConstants.SINGLE_COLUMN, QueryConstants.AGG_TIMESTAMP, value, 0, value.length);
                        results.add(keyValue);
                        // If we're at an aggregation boundary, reset the
                        // aggregators and
                        // aggregate with the current result (which is not a part of
                        // the returned result).
                        if (aggBoundary) {
                            aggregators.reset(rowAggregators);
                            aggregators.aggregate(rowAggregators, tuple);
                            rowAggregators = cache.get(key);
                            if (rowAggregators != null) {
                                // key was contained in in-memory cache, clean it up
                                cache.remove(key);
                            }
                            currentKey = key;
                        }
                    }
                    // Continue if there are more
                    if (scannerIter.hasNext() || aggBoundary) { return true; }
                }

                // return in memory elements
                if (cacheIter == null) {
                    cacheIter = cache.entrySet().iterator();
                }
                if (cacheIter.hasNext()) {
                    Entry<ImmutableBytesWritable, Aggregator[]> entry = cacheIter.next();
                    byte[] value = aggregators.toBytes(entry.getValue());
                    currentKey = entry.getKey();
                    KeyValue keyValue = KeyValueUtil.newKeyValue(currentKey.get(), currentKey.getOffset(),
                            currentKey.getLength(), QueryConstants.SINGLE_COLUMN_FAMILY, QueryConstants.SINGLE_COLUMN,
                            QueryConstants.AGG_TIMESTAMP, value, 0, value.length);
                    results.add(keyValue);
                    return true;
                }

                currentKey = null;
                return false;
            }

            @Override
            public long getMaxResultSize() {
                // TODO Auto-generated method stub
                return 0;
            }
        };
    }

    @Override
    public void close() throws IOException {
        // NOOP
    }
}
