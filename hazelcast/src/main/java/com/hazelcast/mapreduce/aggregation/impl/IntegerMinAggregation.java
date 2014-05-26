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

public class IntegerMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, Integer, Integer, Integer, Integer> {

    @Override
    public Collator<Map.Entry<Key, Integer>, Integer> getCollator() {
        return new Collator<Map.Entry<Key, Integer>, Integer>() {
            @Override
            public Integer collate(Iterable<Map.Entry<Key, Integer>> values) {
                int min = Integer.MAX_VALUE;
                for (Map.Entry<Key, Integer> entry : values) {
                    int value = entry.getValue();
                    if (value < min) {
                        min = value;
                    }
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Integer> getMapper(Supplier<Key, Value, Integer> supplier) {
        return new SupplierConsumingMapper<Key, Value, Integer>(supplier);
    }

    @Override
    public CombinerFactory<Key, Integer, Integer> getCombinerFactory() {
        return new IntegerMinCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Integer, Integer> getReducerFactory() {
        return new IntegerMinReducerFactory<Key>();
    }

    static final class IntegerMinCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Integer, Integer> {

        @Override
        public Combiner<Key, Integer, Integer> newCombiner(Key key) {
            return new IntegerMinCombiner<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.INTEGER_MIN_COMBINER_FACTORY;
        }
    }

    static final class IntegerMinReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Integer, Integer> {

        @Override
        public Reducer<Key, Integer, Integer> newReducer(Key key) {
            return new IntegerMinReducer<Key>();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.INTEGER_MIN_REDUCER_FACTORY;
        }
    }

    private static final class IntegerMinCombiner<Key>
            extends Combiner<Key, Integer, Integer> {

        private int chunkMin = Integer.MAX_VALUE;

        @Override
        public void combine(Key key, Integer value) {
            if (value < chunkMin) {
                chunkMin = value;
            }
        }

        @Override
        public Integer finalizeChunk() {
            int value = chunkMin;
            chunkMin = Integer.MAX_VALUE;
            return value;
        }
    }

    private static final class IntegerMinReducer<Key>
            extends Reducer<Key, Integer, Integer> {

        private volatile int min = Integer.MAX_VALUE;

        @Override
        public void reduce(Integer value) {
            if (value < min) {
                min = value;
            }
        }

        @Override
        public Integer finalizeReduce() {
            return min;
        }
    }
}
