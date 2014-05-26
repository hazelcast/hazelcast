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
import com.hazelcast.mapreduce.aggregation.Supplier;

import java.util.Map;

public class LongSumAggregation<Key, Value>
        implements AggType<Key, Value, Key, Long, Long, Long, Long> {

    @Override
    public Collator<Map.Entry<Key, Long>, Long> getCollator() {
        return new Collator<Map.Entry<Key, Long>, Long>() {
            @Override
            public Long collate(Iterable<Map.Entry<Key, Long>> values) {
                long sum = 0;
                for (Map.Entry<Key, Long> entry : values) {
                    sum += entry.getValue();
                }
                return sum;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Long> getMapper(Supplier<Key, Value, Long> supplier) {
        return new SupplierConsumingMapper<Key, Value, Long>(supplier);
    }

    @Override
    public CombinerFactory<Key, Long, Long> getCombinerFactory() {
        return new LongSumCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Long, Long> getReducerFactory() {
        return new LongSumReducerFactory<Key>();
    }

    static final class LongSumCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Long, Long> {

        @Override
        public Combiner<Long, Long> newCombiner(Key key) {
            return new LongSumCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_SUM_COMBINER_FACTORY;
        }
    }

    static final class LongSumReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Long, Long> {

        @Override
        public Reducer<Long, Long> newReducer(Key key) {
            return new LongSumReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_SUM_REDUCER_FACTORY;
        }
    }

    private static final class LongSumCombiner
            extends Combiner<Long, Long> {

        private long chunkSum;

        @Override
        public void combine(Long value) {
            chunkSum += value;
        }

        @Override
        public Long finalizeChunk() {
            long value = chunkSum;
            chunkSum = 0;
            return value;
        }
    }

    private static final class LongSumReducer
            extends Reducer<Long, Long> {

        private volatile long sum;

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
