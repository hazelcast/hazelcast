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

public class IntegerAvgAggregation<Key, Value>
        implements Aggregation<Key, Value, Key, Integer, AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>, Integer> {

    @Override
    public Collator<Map.Entry<Key, AvgTuple<Integer, Integer>>, Integer> getCollator() {
        return new Collator<Map.Entry<Key, AvgTuple<Integer, Integer>>, Integer>() {
            @Override
            public Integer collate(Iterable<Map.Entry<Key, AvgTuple<Integer, Integer>>> values) {
                int count = 0;
                int amount = 0;
                for (Map.Entry<Key, AvgTuple<Integer, Integer>> entry : values) {
                    AvgTuple<Integer, Integer> tuple = entry.getValue();
                    count += tuple.getFirst();
                    amount += tuple.getSecond();
                }
                return (int) ((double) amount / count);
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Integer> getMapper(Supplier<Key, Value, Integer> supplier) {
        return new SupplierConsumingMapper<Key, Value, Integer>(supplier);
    }

    @Override
    public CombinerFactory<Key, Integer, AvgTuple<Integer, Integer>> getCombinerFactory() {
        return new IntegerAvgCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>> getReducerFactory() {
        return new IntegerAvgReducerFactory<Key>();
    }

    static final class IntegerAvgCombinerFactory<Key>
            implements CombinerFactory<Key, Integer, AvgTuple<Integer, Integer>> {

        @Override
        public Combiner<Key, Integer, AvgTuple<Integer, Integer>> newCombiner(Key key) {
            return new IntegerAvgCombiner<Key>();
        }
    }

    static final class IntegerAvgReducerFactory<Key>
            implements ReducerFactory<Key, AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>> {

        @Override
        public Reducer<Key, AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>> newReducer(Key key) {
            return new IntegerAvgReducer<Key>();
        }
    }

    private static final class IntegerAvgCombiner<Key>
            extends Combiner<Key, Integer, AvgTuple<Integer, Integer>> {

        private int count;
        private int amount;

        @Override
        public void combine(Key key, Integer value) {
            count++;
            amount += value;
        }

        @Override
        public AvgTuple<Integer, Integer> finalizeChunk() {
            int count = this.count;
            int amount = this.amount;
            this.count = 0;
            this.amount = 0;
            return new AvgTuple<Integer, Integer>(count, amount);
        }
    }

    private static final class IntegerAvgReducer<Key>
            extends Reducer<Key, AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>> {

        private volatile int count;
        private volatile int amount;

        @Override
        public void reduce(AvgTuple<Integer, Integer> value) {
            count += value.getFirst();
            amount += value.getSecond();
        }

        @Override
        public AvgTuple<Integer, Integer> finalizeReduce() {
            return new AvgTuple<Integer, Integer>(count, amount);
        }
    }
}
