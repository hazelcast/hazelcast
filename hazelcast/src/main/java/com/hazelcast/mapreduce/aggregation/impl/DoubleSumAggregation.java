/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.BinaryInterface;

import java.util.Map;

/**
 * The predefined sum aggregation for values of type double.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class DoubleSumAggregation<Key, Value>
        implements AggType<Key, Value, Key, Double, Double, Double, Double> {

    @Override
    public Collator<Map.Entry<Key, Double>, Double> getCollator() {
        return new Collator<Map.Entry<Key, Double>, Double>() {
            @Override
            public Double collate(Iterable<Map.Entry<Key, Double>> values) {
                double sum = 0;
                for (Map.Entry<Key, Double> entry : values) {
                    sum += entry.getValue();
                }
                return sum;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Double> getMapper(Supplier<Key, Value, Double> supplier) {
        return new SupplierConsumingMapper<Key, Value, Double>(supplier);
    }

    @Override
    public CombinerFactory<Key, Double, Double> getCombinerFactory() {
        return new DoubleSumCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Double, Double> getReducerFactory() {
        return new DoubleSumReducerFactory<Key>();
    }

    /**
     * Sum CombinerFactory for type double
     *
     * @param <Key> the key type
     */
    @BinaryInterface
    static final class DoubleSumCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Double, Double> {

        @Override
        public Combiner<Double, Double> newCombiner(Key key) {
            return new DoubleSumCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_SUM_COMBINER_FACTORY;
        }
    }

    /**
     * Sum ReducerFactory for type double
     *
     * @param <Key> the key type
     */
    @BinaryInterface
    static final class DoubleSumReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Double, Double> {

        @Override
        public Reducer<Double, Double> newReducer(Key key) {
            return new DoubleSumReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_SUM_REDUCER_FACTORY;
        }
    }

    /**
     * Sum Combiner for type double
     */
    private static final class DoubleSumCombiner
            extends Combiner<Double, Double> {

        private double chunkSum;

        @Override
        public void combine(Double value) {
            chunkSum += value;
        }

        @Override
        public Double finalizeChunk() {
            double value = chunkSum;
            chunkSum = 0;
            return value;
        }
    }

    /**
     * Sum Reducer for type double
     */
    private static final class DoubleSumReducer
            extends Reducer<Double, Double> {

        private double sum;

        @Override
        public void reduce(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeReduce() {
            return sum;
        }
    }
}
