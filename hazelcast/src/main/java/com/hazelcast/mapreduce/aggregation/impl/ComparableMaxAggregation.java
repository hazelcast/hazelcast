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
 * The predefined maximum aggregation for values of type {@link java.lang.Comparable}.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class ComparableMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, Comparable, Comparable, Comparable, Comparable> {

    @Override
    public Collator<Map.Entry<Key, Comparable>, Comparable> getCollator() {
        return new Collator<Map.Entry<Key, Comparable>, Comparable>() {
            @Override
            public Comparable collate(Iterable<Map.Entry<Key, Comparable>> values) {
                Comparable max = null;
                for (Map.Entry<Key, Comparable> entry : values) {
                    Comparable value = entry.getValue();
                    if (max == null || value.compareTo(max) > 0) {
                        max = value;
                    }
                }
                return max;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, Comparable> getMapper(Supplier<Key, Value, Comparable> supplier) {
        return new SupplierConsumingMapper<Key, Value, Comparable>(supplier);
    }

    @Override
    public CombinerFactory<Key, Comparable, Comparable> getCombinerFactory() {
        return new ComparableMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, Comparable, Comparable> getReducerFactory() {
        return new ComparableMaxReducerFactory<Key>();
    }

    /**
     * Maximum CombinerFactory for type {@link java.lang.Comparable}
     *
     * @param <Key> the key type
     */
    static final class ComparableMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, Comparable, Comparable> {

        @Override
        public Combiner<Comparable, Comparable> newCombiner(Key key) {
            return new ComparableMaxCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MAX_COMBINER_FACTORY;
        }
    }

    /**
     * Maximum ReducerFactory for type {@link java.lang.Comparable}
     *
     * @param <Key> the key type
     */
    static final class ComparableMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, Comparable, Comparable> {

        @Override
        public Reducer<Comparable, Comparable> newReducer(Key key) {
            return new ComparableMaxReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.COMPARABLE_MAX_REDUCER_FACTORY;
        }
    }

    /**
     * Maximum Combiner for type {@link java.lang.Comparable}
     */
    private static final class ComparableMaxCombiner
            extends Combiner<Comparable, Comparable> {

        private Comparable max;

        @Override
        public void combine(Comparable value) {
            if (max == null || value.compareTo(max) > 0) {
                max = value;
            }
        }

        @Override
        public Comparable finalizeChunk() {
            Comparable value = max;
            max = null;
            return value;
        }
    }

    /**
     * Maximum Reducer for type {@link java.lang.Comparable}
     */
    private static final class ComparableMaxReducer
            extends Reducer<Comparable, Comparable> {

        private Comparable max;

        @Override
        public void reduce(Comparable value) {
            if (max == null || value.compareTo(max) > 0) {
                max = value;
            }
        }

        @Override
        public Comparable finalizeReduce() {
            return max;
        }
    }
}
