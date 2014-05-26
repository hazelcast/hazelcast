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

public class LongMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, Long, Long, Long, Long> {

    @Override
    public Collator<Map.Entry<Key, Long>, Long> getCollator() {
        return new Collator<Map.Entry<Key, Long>, Long>() {
            @Override
            public Long collate(Iterable<Map.Entry<Key, Long>> values) {
                long max = Integer.MIN_VALUE;
                for (Map.Entry<Key, Long> entry : values) {
                    long value = entry.getValue();
                    if (value > max) {
                        max = value;
                    }
                }
                return max;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Long> getMapper(Supplier<Key, Value, Long> supplier) {
        return new SupplierConsumingMapper<Key, Value, Long>(supplier);
    }

    @Override
    public CombinerFactory<Key, Long, Long> getCombinerFactory() {
        return new LongMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Long, Long> getReducerFactory() {
        return new LongMaxReducerFactory<Key>();
    }

    static final class LongMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Long, Long> {

        @Override
        public Combiner<Key, Long, Long> newCombiner(Key key) {
            return new LongMaxCombiner<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_MAX_COMBINER_FACTORY;
        }
    }

    static final class LongMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Long, Long> {

        @Override
        public Reducer<Key, Long, Long> newReducer(Key key) {
            return new LongMaxReducer<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_MAX_REDUCER_FACTORY;
        }
    }

    private static final class LongMaxCombiner<Key>
            extends Combiner<Key, Long, Long> {

        private long chunkMax = Long.MIN_VALUE;

        @Override
        public void combine(Key key, Long value) {
            if (value > chunkMax) {
                chunkMax = value;
            }
        }

        @Override
        public Long finalizeChunk() {
            long value = chunkMax;
            chunkMax = Long.MIN_VALUE;
            return value;
        }
    }

    private static final class LongMaxReducer<Key>
            extends Reducer<Key, Long, Long> {

        private volatile long max = Long.MIN_VALUE;

        @Override
        public void reduce(Long value) {
            if (value > max) {
                max = value;
            }
        }

        @Override
        public Long finalizeReduce() {
            return max;
        }
    }
}
