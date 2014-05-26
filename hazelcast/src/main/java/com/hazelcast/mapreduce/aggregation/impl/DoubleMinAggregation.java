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

public class DoubleMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, Double, Double, Double, Double> {

    @Override
    public Collator<Map.Entry<Key, Double>, Double> getCollator() {
        return new Collator<Map.Entry<Key, Double>, Double>() {
            @Override
            public Double collate(Iterable<Map.Entry<Key, Double>> values) {
                double min = Double.MAX_VALUE;
                for (Map.Entry<Key, Double> entry : values) {
                    double value = entry.getValue();
                    if (value < min) {
                        min = value;
                    }
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Double> getMapper(Supplier<Key, Value, Double> supplier) {
        return new SupplierConsumingMapper<Key, Value, Double>(supplier);
    }

    @Override
    public CombinerFactory<Key, Double, Double> getCombinerFactory() {
        return new DoubleMinCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Double, Double> getReducerFactory() {
        return new DoubleMinReducerFactory<Key>();
    }

    static final class DoubleMinCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Double, Double> {

        @Override
        public Combiner<Double, Double> newCombiner(Key key) {
            return new DoubleMinCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_MIN_COMBINER_FACTORY;
        }
    }

    static final class DoubleMinReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Double, Double> {

        @Override
        public Reducer<Double, Double> newReducer(Key key) {
            return new DoubleMinReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.DOUBLE_MIN_REDUCER_FACTORY;
        }
    }

    private static final class DoubleMinCombiner
            extends Combiner<Double, Double> {

        private double chunkMin = Double.MAX_VALUE;

        @Override
        public void combine(Double value) {
            if (value < chunkMin) {
                chunkMin = value;
            }
        }

        @Override
        public Double finalizeChunk() {
            double value = chunkMin;
            chunkMin = Double.MAX_VALUE;
            return value;
        }
    }

    private static final class DoubleMinReducer
            extends Reducer<Double, Double> {

        private volatile double min = Double.MAX_VALUE;

        @Override
        public void reduce(Double value) {
            if (value < min) {
                min = value;
            }
        }

        @Override
        public Double finalizeReduce() {
            return min;
        }
    }
}
