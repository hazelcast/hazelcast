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

import java.math.BigDecimal;
import java.util.Map;

public class BigDecimalAvgAggregation<Key, Value>
        implements AggType<Key, Value, Key, BigDecimal, AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>, BigDecimal> {

    @Override
    public Collator<Map.Entry<Key, AvgTuple<Long, BigDecimal>>, BigDecimal> getCollator() {
        return new Collator<Map.Entry<Key, AvgTuple<Long, BigDecimal>>, BigDecimal>() {
            @Override
            public BigDecimal collate(Iterable<Map.Entry<Key, AvgTuple<Long, BigDecimal>>> values) {
                long count = 0;
                BigDecimal amount = BigDecimal.ZERO;
                for (Map.Entry<Key, AvgTuple<Long, BigDecimal>> entry : values) {
                    AvgTuple<Long, BigDecimal> tuple = entry.getValue();
                    count += tuple.getFirst();
                    amount = amount.add(tuple.getSecond());
                }
                return amount.divide(BigDecimal.valueOf(count));
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, BigDecimal> getMapper(Supplier<Key, Value, BigDecimal> supplier) {
        return new SupplierConsumingMapper<Key, Value, BigDecimal>(supplier);
    }

    @Override
    public CombinerFactory<Key, BigDecimal, AvgTuple<Long, BigDecimal>> getCombinerFactory() {
        return new BigDecimalAvgCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>> getReducerFactory() {
        return new BigDecimalAvgReducerFactory<Key>();
    }

    static final class BigDecimalAvgCombinerFactory<Key>
            implements CombinerFactory<Key, BigDecimal, AvgTuple<Long, BigDecimal>> {

        @Override
        public Combiner<Key, BigDecimal, AvgTuple<Long, BigDecimal>> newCombiner(Key key) {
            return new BigDecimalAvgCombiner<Key>();
        }
    }

    static final class BigDecimalAvgReducerFactory<Key>
            implements ReducerFactory<Key, AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>> {

        @Override
        public Reducer<Key, AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>> newReducer(Key key) {
            return new BigDecimalAvgReducer<Key>();
        }
    }

    private static final class BigDecimalAvgCombiner<Key>
            extends Combiner<Key, BigDecimal, AvgTuple<Long, BigDecimal>> {

        private long count;
        private BigDecimal amount = BigDecimal.ZERO;

        @Override
        public void combine(Key key, BigDecimal value) {
            count++;
            amount = amount.add(value);
        }

        @Override
        public AvgTuple<Long, BigDecimal> finalizeChunk() {
            long count = this.count;
            BigDecimal amount = this.amount;
            this.count = 0;
            this.amount = BigDecimal.ZERO;
            return new AvgTuple<Long, BigDecimal>(count, amount);
        }
    }

    private static final class BigDecimalAvgReducer<Key>
            extends Reducer<Key, AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>> {

        private volatile long count;
        private volatile BigDecimal amount = BigDecimal.ZERO;

        @Override
        public void reduce(AvgTuple<Long, BigDecimal> value) {
            count += value.getFirst();
            amount = amount.add(value.getSecond());
        }

        @Override
        public AvgTuple<Long, BigDecimal> finalizeReduce() {
            return new AvgTuple<Long, BigDecimal>(count, amount);
        }
    }
}
