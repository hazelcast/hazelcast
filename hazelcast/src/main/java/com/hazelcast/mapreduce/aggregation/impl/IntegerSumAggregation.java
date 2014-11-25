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
 * The predefined sum aggregation for values of type integer.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class IntegerSumAggregation<Key, Value>
        implements AggType<Key, Value, Key, Integer, Integer, Integer, Integer> {

    @Override
    public Collator<Map.Entry<Key, Integer>, Integer> getCollator() {
        return new Collator<Map.Entry<Key, Integer>, Integer>() {
            @Override
            public Integer collate(Iterable<Map.Entry<Key, Integer>> values) {
                int sum = 0;
                for (Map.Entry<Key, Integer> entry : values) {
                    sum += entry.getValue();
                }
                return sum;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Integer> getMapper(Supplier<Key, Value, Integer> supplier) {
        return new SupplierConsumingMapper<Key, Value, Integer>(supplier);
    }

    @Override
    public CombinerFactory<Key, Integer, Integer> getCombinerFactory() {
        return new IntegerSumCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Integer, Integer> getReducerFactory() {
        return new IntegerSumReducerFactory<Key>();
    }

    /**
     * Sum CombinerFactory for type integer
     *
     * @param <Key> the key type
     */
    static final class IntegerSumCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Integer, Integer> {

        @Override
        public Combiner<Integer, Integer> newCombiner(Key key) {
            return new IntegerSumCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.INTEGER_SUM_COMBINER_FACTORY;
        }
    }

    /**
     * Sum ReducerFactory for type integer
     *
     * @param <Key> the key type
     */
    static final class IntegerSumReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Integer, Integer> {

        @Override
        public Reducer<Integer, Integer> newReducer(Key key) {
            return new IntegerSumReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.INTEGER_SUM_REDUCER_FACTORY;
        }
    }

    /**
     * Sum Combiner for type integer
     */
    private static final class IntegerSumCombiner
            extends Combiner<Integer, Integer> {

        private int chunkSum;

        @Override
        public void combine(Integer value) {
            chunkSum += value;
        }

        @Override
        public Integer finalizeChunk() {
            int value = chunkSum;
            chunkSum = 0;
            return value;
        }
    }

    /**
     * Sum Reducer for type integer
     */
    private static final class IntegerSumReducer
            extends Reducer<Integer, Integer> {

        private int sum;

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}
