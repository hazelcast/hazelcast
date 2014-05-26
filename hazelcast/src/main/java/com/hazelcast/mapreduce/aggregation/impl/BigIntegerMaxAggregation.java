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

public class BigIntegerMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> {

    @Override
    public Collator<Map.Entry<Key, BigInteger>, BigInteger> getCollator() {
        return new Collator<Map.Entry<Key, BigInteger>, BigInteger>() {
            @Override
            public BigInteger collate(Iterable<Map.Entry<Key, BigInteger>> values) {
                BigInteger max = null;
                for (Map.Entry<Key, BigInteger> entry : values) {
                    BigInteger value = entry.getValue();
                    max = max == null ? value : value.max(max);
                }
                return max;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, BigInteger> getMapper(Supplier<Key, Value, BigInteger> supplier) {
        return new SupplierConsumingMapper<Key, Value, BigInteger>(supplier);
    }

    @Override
    public CombinerFactory<Key, BigInteger, BigInteger> getCombinerFactory() {
        return new BigIntegerMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, BigInteger, BigInteger> getReducerFactory() {
        return new BigIntegerMaxReducerFactory<Key>();
    }

    static final class BigIntegerMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Combiner<BigInteger, BigInteger> newCombiner(Key key) {
            return new BigIntegerMaxCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_INTEGER_MAX_COMBINER_FACTORY;
        }
    }

    static final class BigIntegerMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Reducer<BigInteger, BigInteger> newReducer(Key key) {
            return new BigIntegerMaxReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_INTEGER_MAX_REDUCER_FACTORY;
        }
    }

    private static final class BigIntegerMaxCombiner
            extends Combiner<BigInteger, BigInteger> {

        private BigInteger max = null;

        @Override
        public void combine(BigInteger value) {
            max = max == null ? value : value.max(max);
        }

        @Override
        public BigInteger finalizeChunk() {
            return max;
        }

        @Override
        public void reset() {
            max = null;
        }
    }

    private static final class BigIntegerMaxReducer
            extends Reducer<BigInteger, BigInteger> {

        private volatile BigInteger max = null;

        @Override
        public void reduce(BigInteger value) {
            max = max == null ? value : value.max(max);
        }

        @Override
        public BigInteger finalizeReduce() {
            return max;
        }
    }
}
