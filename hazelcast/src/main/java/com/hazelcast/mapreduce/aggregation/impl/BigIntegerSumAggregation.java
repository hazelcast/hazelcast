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

import java.math.BigInteger;
import java.util.Map;

public class BigIntegerSumAggregation<Key, Value>
        implements Aggregation<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> {

    @Override
    public Collator<Map.Entry<Key, BigInteger>, BigInteger> getCollator() {
        return new Collator<Map.Entry<Key, BigInteger>, BigInteger>() {
            @Override
            public BigInteger collate(Iterable<Map.Entry<Key, BigInteger>> values) {
                BigInteger sum = BigInteger.ZERO;
                for (Map.Entry<Key, BigInteger> entry : values) {
                    sum = sum.add(entry.getValue());
                }
                return sum;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, BigInteger> getMapper(Supplier<Key, Value, BigInteger> supplier) {
        return new SupplierConsumingMapper<Key, Value, BigInteger>(supplier);
    }

    @Override
    public CombinerFactory<Key, BigInteger, BigInteger> getCombinerFactory() {
        return new BigIntegerSumCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, BigInteger, BigInteger> getReducerFactory() {
        return new BigIntegerSumReducerFactory<Key>();
    }

    static final class BigIntegerSumCombinerFactory<Key>
            implements CombinerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Combiner<Key, BigInteger, BigInteger> newCombiner(Key key) {
            return new BigIntegerSumCombiner<Key>();
        }
    }

    static final class BigIntegerSumReducerFactory<Key>
            implements ReducerFactory<Key, BigInteger, BigInteger> {

        @Override
        public Reducer<Key, BigInteger, BigInteger> newReducer(Key key) {
            return new BigIntegerSumReducer<Key>();
        }
    }

    private static final class BigIntegerSumCombiner<Key>
            extends Combiner<Key, BigInteger, BigInteger> {

        private BigInteger chunkSum = BigInteger.ZERO;

        @Override
        public void combine(Key key, BigInteger value) {
            chunkSum = chunkSum.add(value);
        }

        @Override
        public BigInteger finalizeChunk() {
            BigInteger value = chunkSum;
            chunkSum = BigInteger.ZERO;
            return value;
        }
    }

    private static final class BigIntegerSumReducer<Key>
            extends Reducer<Key, BigInteger, BigInteger> {

        private volatile BigInteger sum = BigInteger.ZERO;

        @Override
        public void reduce(BigInteger value) {
            sum = sum.add(value);
        }

        @Override
        public BigInteger finalizeReduce() {
            return sum;
        }
    }
}
