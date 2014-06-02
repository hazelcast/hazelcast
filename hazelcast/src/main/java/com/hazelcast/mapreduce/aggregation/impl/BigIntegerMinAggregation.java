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

import java.math.BigInteger;
import java.util.Map;

/**
 * The predefined minimum aggregation for values of type {@link java.math.BigInteger}.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class BigIntegerMinAggregation<Key, Value>
        implements AggType<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> {

    @Override
    public Collator<Map.Entry<Key, BigInteger>, BigInteger> getCollator() {
        return new Collator<Map.Entry<Key, BigInteger>, BigInteger>() {
            @Override
            public BigInteger collate(Iterable<Map.Entry<Key, BigInteger>> values) {
                BigInteger min = null;
                for (Map.Entry<Key, BigInteger> entry : values) {
                    BigInteger value = entry.getValue();
                    min = min == null ? value : value.min(min);
                }
                return min;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, BigInteger> getMapper(Supplier<Key, Value, BigInteger> supplier) {
        return new SupplierConsumingMapper<Key, Value, BigInteger>(supplier);
    }

    @Override
    public CombinerFactory<Key, BigInteger, BigInteger> getCombinerFactory() {
        return new BigIntegerMinCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, BigInteger, BigInteger> getReducerFactory() {
        return new BigIntegerMinReducerFactory<Key>();
    }

    /**
     * Minimum CombinerFactory for type {@link java.math.BigInteger}
     *
     * @param <Key> the key type
     */
    static final class BigIntegerMinCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Combiner<BigInteger, BigInteger> newCombiner(Key key) {
            return new BigIntegerMinCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_INTEGER_MIN_COMBINER_FACTORY;
        }
    }

    /**
     * Minimum ReducerFactory for type {@link java.math.BigInteger}
     *
     * @param <Key> the key type
     */
    static final class BigIntegerMinReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Reducer<BigInteger, BigInteger> newReducer(Key key) {
            return new BigIntegerMinReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_INTEGER_MIN_REDUCER_FACTORY;
        }
    }

    /**
     * Minimum Combiner for type {@link java.math.BigInteger}
     */
    private static final class BigIntegerMinCombiner
            extends Combiner<BigInteger, BigInteger> {

        private BigInteger min;

        @Override
        public void combine(BigInteger value) {
            min = min == null ? value : value.min(min);
        }

        @Override
        public BigInteger finalizeChunk() {
            return min;
        }

        @Override
        public void reset() {
            min = null;
        }
    }

    /**
     * Minimum Reducer for type {@link java.math.BigInteger}
     */
    private static final class BigIntegerMinReducer
            extends Reducer<BigInteger, BigInteger> {

        private volatile BigInteger min;

        @Override
        public void reduce(BigInteger value) {
            min = min == null ? value : value.min(min);
        }

        @Override
        public BigInteger finalizeReduce() {
            return min;
        }
    }
}
