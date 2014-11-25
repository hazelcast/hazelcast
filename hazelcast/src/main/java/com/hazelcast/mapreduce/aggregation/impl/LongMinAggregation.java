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

/**
 * The predefined minimum aggregation for values of type long.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class LongMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, Long, Long, Long, Long> {

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

    /**
     * Minimum CombinerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class LongMinCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Long, Long> {

        @Override
        public Combiner<Long, Long> newCombiner(Key key) {
            return new LongMinCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_MIN_COMBINER_FACTORY;
        }
    }

    /**
     * Minimum ReducerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class LongMinReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Long, Long> {

        @Override
        public Reducer<Long, Long> newReducer(Key key) {
            return new LongMinReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_MIN_REDUCER_FACTORY;
        }
    }

    /**
     * Minimum Combiner for type long
     */
    private static final class LongMinCombiner
            extends Combiner<Long, Long> {

        private long chunkMin = Long.MAX_VALUE;

        @Override
        public void combine(Long value) {
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

    /**
     * Minimum Reducer for type long
     */
    private static final class LongMinReducer
            extends Reducer<Long, Long> {

        private long min = Long.MAX_VALUE;

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
