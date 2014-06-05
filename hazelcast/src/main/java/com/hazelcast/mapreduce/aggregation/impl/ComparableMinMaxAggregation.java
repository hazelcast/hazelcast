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
 * The predefined minimum/maximum aggregation for values of type long.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class ComparableMinMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, Comparable, MinMaxTuple<Comparable>,
                           MinMaxTuple<Comparable>, MinMaxTuple<Comparable>> {

    @Override
    public Collator<Map.Entry<Key, MinMaxTuple<Comparable>>, MinMaxTuple<Comparable>> getCollator() {
        return new Collator<Map.Entry<Key, MinMaxTuple<Comparable>>, MinMaxTuple<Comparable>>() {
            @Override
            public MinMaxTuple<Comparable> collate(Iterable<Map.Entry<Key, MinMaxTuple<Comparable>>> values) {
                MinMaxTuple<Comparable> result = new MinMaxTuple<Comparable>();
                for (Map.Entry<Key, MinMaxTuple<Comparable>> entry : values) {
                    MinMaxTuple<Comparable> value = entry.getValue();
                    result.update(value);
                }

                return result;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Comparable> getMapper(Supplier<Key, Value, Comparable> supplier) {
        return new SupplierConsumingMapper<Key, Value, Comparable>(supplier);
    }

    @Override
    public CombinerFactory<Key, Comparable, MinMaxTuple<Comparable>> getCombinerFactory() {
        return new ComparableMinMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, MinMaxTuple<Comparable>, MinMaxTuple<Comparable>> getReducerFactory() {
        return new ComparableMinMaxReducerFactory<Key>();
    }

    /**
     * Minimum/Maximum CombinerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class ComparableMinMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Comparable, MinMaxTuple<Comparable>> {

        @Override
        public Combiner<Comparable, MinMaxTuple<Comparable>> newCombiner(Key key) {
            return new ComparableMinMaxCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MINMAX_COMBINER_FACTORY;
        }
    }

    /**
     * Minimum/Maximum ReducerFactory for type long
     *
     * @param <Key> the key type
     */
    static final class ComparableMinMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, MinMaxTuple<Comparable>, MinMaxTuple<Comparable>> {

        @Override
        public Reducer<MinMaxTuple<Comparable>, MinMaxTuple<Comparable>> newReducer(Key key) {
            return new ComparableMinMaxReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MINMAX_REDUCER_FACTORY;
        }
    }

    /**
     * Minimum/Maximum Combiner for type long
     */
    private static final class ComparableMinMaxCombiner
            extends Combiner<Comparable, MinMaxTuple<Comparable>> {

        private MinMaxTuple<Comparable> chunkMinMax = new MinMaxTuple<Comparable>();

        @Override
        public void combine(Comparable value) {
            chunkMinMax.update(value);
        }

        @Override
        public MinMaxTuple<Comparable> finalizeChunk() {
            MinMaxTuple<Comparable> value = chunkMinMax;
            chunkMinMax = new MinMaxTuple<Comparable>();
            return value;
        }
    }

    /**
     * Minimum/Maximum Reducer for type long
     */
    private static final class ComparableMinMaxReducer
            extends Reducer<MinMaxTuple<Comparable>, MinMaxTuple<Comparable>> {

        private MinMaxTuple<Comparable> minMax = new MinMaxTuple<Comparable>();

        @Override
        public void reduce(MinMaxTuple<Comparable> value) {
            minMax.update(value);
        }

        @Override
        public MinMaxTuple<Comparable> finalizeReduce() {
            return minMax;
        }
    }
}
