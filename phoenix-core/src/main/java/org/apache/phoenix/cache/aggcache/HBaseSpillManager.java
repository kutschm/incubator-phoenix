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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class HBaseSpillManager implements ISpillManager<Tuple> {
    private static final int BATCH_SIZE = 100;
    private final HTableInterface htable;
    private final ServerAggregators aggregators;
    private final List<Put> puts;
    private boolean initialzed = false;
    long ts = 1;

    public HBaseSpillManager(final RegionCoprocessorEnvironment env, ServerAggregators aggs, List<Expression> exprs) {
        this.aggregators = aggs;
        puts = Lists.newArrayListWithExpectedSize(BATCH_SIZE);
        try {
            htable = env.getTable(TableName.valueOf(Bytes.toBytes("SYSTEM.SPILLABLE_CACHE")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init() {
        // TODO lazily create the spill table
        initialzed = true;
    }

    @Override
    public boolean initialized() {
        return initialzed;
    }

    @Override
    public void spill(ImmutableBytesWritable key, Aggregator[] value) throws IOException {
        // Create a Put and add the key and value that you used in your SpillableGroupByCache
        Put put = new Put(key.copyBytes(), ts++); // Increment timestamp for every Put
        Aggregator[] rowAggregators = value;
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                aggregators.toBytes(rowAggregators));
        puts.add(put);
        if (puts.size() == BATCH_SIZE) {
            try {
                htable.batch(puts);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            puts.clear();
        }
    }

    @Override
    public Aggregator[] loadEntry(ImmutableBytesWritable key) throws IOException {
        throw new UnsupportedOperationException("Method not supported for this type of cache");
    }

    @Override
    public Iterator<Tuple> newDataIterator() {

        // flush the last puts first
        if (puts.size() != 0) {
            try {
                htable.batch(puts);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ResultScanner scanner = null;
        Scan scan = new Scan();
        scan.setRaw(true);
        // We may need to "preallocate" a chunk of timestamps from an Atomic long
        // so that multiple, simultaneous spillages don't tromp on each other. We can
        // do this after perf testing, though.
        try {
            scan = scan.setMaxVersions();
            scan = scan.setTimeRange(0L, Long.MAX_VALUE);
            // If we can keep the data local, this scanner will bypass the RPC and deserialization
            // hit we'd otherwise take.
            scanner = htable.getScanner(scan);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new HBaseSpillIterator(scanner);
    }

    private final class HBaseSpillIterator implements Iterator<Tuple> {

        Iterator<Result> resIter;

        public HBaseSpillIterator(ResultScanner rs) {
            this.resIter = rs.iterator();
        }

        @Override
        public boolean hasNext() {
            return resIter.hasNext();
        }

        @Override
        public Tuple next() {
            // Results are potentially returned even when the return
            // value of s.next is false
            // since this is an indication of whether or not there
            // are more values after the
            // ones returned
            return new ResultTuple(resIter.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove is not supported for this type of iterator");
        }
    }

    @Override
    public void close() throws IOException {
        Closeables.closeQuietly(htable);
    }
}
