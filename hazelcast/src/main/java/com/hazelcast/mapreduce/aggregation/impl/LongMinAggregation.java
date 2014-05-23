/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;

import java.util.Map;

public class LongMinAggregation<Key, Value>
        implements Aggregation<Key, Value, Key, Long, Long, Long, Long> {

    @Override
    public Collator<Map.Entry<Key, Long>, Long> getCollator() {
        return new Collator<Map.Entry<Key, Long>, Long>() {
            @Override
            public Long collate(Iterable<Map.Entry<Key, Long>> values) {
                long min = Long.MAX_VALUE;
                for (Map.Entry<Key, Long> entry : values) {
                    long value = entry.getValue();
                    if (value < min) {
                        min = value;
                    }
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Long> getMapper(Supplier<Key, Value, Long> supplier) {
        return new SupplierConsumingMapper<Key, Value, Long>(supplier);
    }

    @Override
    public CombinerFactory<Key, Long, Long> getCombinerFactory() {
        return new LongMinCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Long, Long> getReducerFactory() {
        return new LongMinReducerFactory<Key>();
    }

    static final class LongMinCombinerFactory<Key>
            implements CombinerFactory<Key, Long, Long> {

        @Override
        public Combiner<Key, Long, Long> newCombiner(Key key) {
            return new LongMinCombiner<Key>();
        }
    }

    static final class LongMinReducerFactory<Key>
            implements ReducerFactory<Key, Long, Long> {

        @Override
        public Reducer<Key, Long, Long> newReducer(Key key) {
            return new LongMinReducer<Key>();
        }
    }

    private static final class LongMinCombiner<Key>
            extends Combiner<Key, Long, Long> {

        private long chunkMin = Long.MAX_VALUE;

        @Override
        public void combine(Key key, Long value) {
            if (value < chunkMin) {
                chunkMin = value;
            }
        }

        @Override
        public Long finalizeChunk() {
            long value = chunkMin;
            chunkMin = Long.MAX_VALUE;
            return value;
        }
    }

    private static final class LongMinReducer<Key>
            extends Reducer<Key, Long, Long> {

        private volatile long min = Long.MAX_VALUE;

        @Override
        public void reduce(Long value) {
            if (value < min) {
                min = value;
            }
        }

        @Override
        public Long finalizeReduce() {
            return min;
        }
    }
}
