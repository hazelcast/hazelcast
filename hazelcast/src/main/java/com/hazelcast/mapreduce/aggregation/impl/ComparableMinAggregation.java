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
 * The predefined minimum aggregation for values of type {@link java.lang.Comparable}.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class ComparableMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, Comparable, Comparable, Comparable, Comparable> {

    @Override
    public Collator<Map.Entry<Key, Comparable>, Comparable> getCollator() {
        return new Collator<Map.Entry<Key, Comparable>, Comparable>() {
            @Override
            public Comparable collate(Iterable<Map.Entry<Key, Comparable>> values) {
                Comparable min = null;
                for (Map.Entry<Key, Comparable> entry : values) {
                    Comparable value = entry.getValue();
                    if (min == null || value.compareTo(min) < 0) {
                        min = value;
                    }
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Comparable> getMapper(Supplier<Key, Value, Comparable> supplier) {
        return new SupplierConsumingMapper<Key, Value, Comparable>(supplier);
    }

    @Override
    public CombinerFactory<Key, Comparable, Comparable> getCombinerFactory() {
        return new ComparableMinCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Comparable, Comparable> getReducerFactory() {
        return new ComparableMinReducerFactory<Key>();
    }

    /**
     * Minimum CombinerFactory for type {@link java.lang.Comparable}
     *
     * @param <Key> the key type
     */
    static final class ComparableMinCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Comparable, Comparable> {

        @Override
        public Combiner<Comparable, Comparable> newCombiner(Key key) {
            return new ComparableMinCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MIN_COMBINER_FACTORY;
        }
    }

    /**
     * Minimum ReducerFactory for type {@link java.lang.Comparable}
     *
     * @param <Key> the key type
     */
    static final class ComparableMinReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Comparable, Comparable> {

        @Override
        public Reducer<Comparable, Comparable> newReducer(Key key) {
            return new ComparableMinReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MIN_REDUCER_FACTORY;
        }
    }

    /**
     * Minimum Combiner for type {@link java.lang.Comparable}
     */
    private static final class ComparableMinCombiner
            extends Combiner<Comparable, Comparable> {

        private Comparable min;

        @Override
        public void combine(Comparable value) {
            if (min == null || value.compareTo(min) < 0) {
                min = value;
            }
        }

        @Override
        public Comparable finalizeChunk() {
            Comparable value = min;
            min = null;
            return value;
        }
    }

    /**
     * Minimum Reducer for type {@link java.lang.Comparable}
     */
    private static final class ComparableMinReducer
            extends Reducer<Comparable, Comparable> {

        private Comparable min;

        @Override
        public void reduce(Comparable value) {
            if (min == null || value.compareTo(min) < 0) {
                min = value;
            }
        }

        @Override
        public Comparable finalizeReduce() {
            return min;
        }
    }
}
