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
 * The predefined average aggregation for values of type long.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class LongAvgAggregation<Key, Value>
        implements AggType<Key, Value, Key, Long, AvgTuple<Long, Long>, AvgTuple<Long, Long>, Long> {

    @Override
    public Collator<Map.Entry<Key, AvgTuple<Long, Long>>, Long> getCollator() {
        return new Collator<Map.Entry<Key, AvgTuple<Long, Long>>, Long>() {
            @Override
            public Long collate(Iterable<Map.Entry<Key, AvgTuple<Long, Long>>> values) {
                long count = 0;
                long amount = 0;
                for (Map.Entry<Key, AvgTuple<Long, Long>> entry : values) {
                    AvgTuple<Long, Long> tuple = entry.getValue();
                    count += tuple.getFirst();
                    amount += tuple.getSecond();
                }
                return (long) ((double) amount / count);
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Long> getMapper(Supplier<Key, Value, Long> supplier) {
        return new SupplierConsumingMapper<Key, Value, Long>(supplier);
    }

    @Override
    public CombinerFactory<Key, Long, AvgTuple<Long, Long>> getCombinerFactory() {
        return new LongAvgCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, AvgTuple<Long, Long>, AvgTuple<Long, Long>> getReducerFactory() {
        return new LongAvgReducerFactory<Key>();
    }

    /**
     * Average CombinerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class LongAvgCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Long, AvgTuple<Long, Long>> {

        @Override
        public Combiner<Long, AvgTuple<Long, Long>> newCombiner(Key key) {
            return new LongAvgCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_AVG_COMBINER_FACTORY;
        }
    }

    /**
     * Average ReducerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class LongAvgReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, AvgTuple<Long, Long>, AvgTuple<Long, Long>> {

        @Override
        public Reducer<AvgTuple<Long, Long>, AvgTuple<Long, Long>> newReducer(Key key) {
            return new LongAvgReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.LONG_AVG_REDUCER_FACTORY;
        }
    }

    /**
     * Average Combiner for type long
     */
    private static final class LongAvgCombiner
            extends Combiner<Long, AvgTuple<Long, Long>> {

        private long count;
        private long amount;

        @Override
        public void combine(Long value) {
            count++;
            amount += value;
        }

        @Override
        public AvgTuple<Long, Long> finalizeChunk() {
            long count = this.count;
            long amount = this.amount;
            this.count = 0;
            this.amount = 0;
            return new AvgTuple<Long, Long>(count, amount);
        }
    }

    /**
     * Average Reducer for type long
     */
    private static final class LongAvgReducer
            extends Reducer<AvgTuple<Long, Long>, AvgTuple<Long, Long>> {

        private volatile long count;
        private volatile long amount;

        @Override
        public void reduce(AvgTuple<Long, Long> value) {
            count += value.getFirst();
            amount += value.getSecond();
        }

        @Override
        public AvgTuple<Long, Long> finalizeReduce() {
            return new AvgTuple<Long, Long>(count, amount);
        }
    }
}
