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
 * The predefined maximum aggregation for values of type double.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class DoubleMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, Double, Double, Double, Double> {

    @Override
    public Collator<Map.Entry<Key, Double>, Double> getCollator() {
        return new Collator<Map.Entry<Key, Double>, Double>() {
            @Override
            public Double collate(Iterable<Map.Entry<Key, Double>> values) {
                double max = -Double.MAX_VALUE;
                for (Map.Entry<Key, Double> entry : values) {
                    double value = entry.getValue();
                    if (value > max) {
                        max = value;
                    }
                }
                return max;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Double> getMapper(Supplier<Key, Value, Double> supplier) {
        return new SupplierConsumingMapper<Key, Value, Double>(supplier);
    }

    @Override
    public CombinerFactory<Key, Double, Double> getCombinerFactory() {
        return new DoubleMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Double, Double> getReducerFactory() {
        return new DoubleMaxReducerFactory<Key>();
    }

    /**
     * Maximum CombinerFactory for type double
     *
     * @param <Key> the key type
     */
    static final class DoubleMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Double, Double> {

        @Override
        public Combiner<Double, Double> newCombiner(Key key) {
            return new DoubleMaxCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_MAX_COMBINER_FACTORY;
        }
    }

    /**
     * Maximum ReducerFactory for type double
     *
     * @param <Key> the key type
     */
    static final class DoubleMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Double, Double> {

        @Override
        public Reducer<Double, Double> newReducer(Key key) {
            return new DoubleMaxReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_MAX_REDUCER_FACTORY;
        }
    }

    /**
     * Maximum Combiner for type double
     */
    private static final class DoubleMaxCombiner
            extends Combiner<Double, Double> {

        private double chunkMax = -Double.MAX_VALUE;

        @Override
        public void combine(Double value) {
            if (value > chunkMax) {
                chunkMax = value;
            }
        }

        @Override
        public Double finalizeChunk() {
            double value = chunkMax;
            chunkMax = -Double.MAX_VALUE;
            return value;
        }
    }

    /**
     * Maximum Reducer for type double
     */
    private static final class DoubleMaxReducer
            extends Reducer<Double, Double> {

        private volatile double max = -Double.MAX_VALUE;

        @Override
        public void reduce(Double value) {
            if (value > max) {
                max = value;
            }
        }

        @Override
        public Double finalizeReduce() {
            return max;
        }
    }
}
