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

/**
 * The predefined maximum aggregation for values of type {@link java.math.BigDecimal}.
 *
 * @param <Key>   the input key type
 * @param <Value> the input value type
 */
public class BigDecimalMaxAggregation<Key, Value>
        implements AggType<Key, Value, Key, BigDecimal, BigDecimal, BigDecimal, BigDecimal> {

    @Override
    public Collator<Map.Entry<Key, BigDecimal>, BigDecimal> getCollator() {
        return new Collator<Map.Entry<Key, BigDecimal>, BigDecimal>() {
            @Override
            public BigDecimal collate(Iterable<Map.Entry<Key, BigDecimal>> values) {
                BigDecimal max = null;
                for (Map.Entry<Key, BigDecimal> entry : values) {
                    BigDecimal value = entry.getValue();
                    max = max == null ? value : value.max(max);
                }
                return max;
            }
        };
    }

    @Override
    public Mapper<Key, Value, Key, BigDecimal> getMapper(Supplier<Key, Value, BigDecimal> supplier) {
        return new SupplierConsumingMapper<Key, Value, BigDecimal>(supplier);
    }

    @Override
    public CombinerFactory<Key, BigDecimal, BigDecimal> getCombinerFactory() {
        return new BigDecimalMaxCombinerFactory<Key>();
    }

    @Override
    public ReducerFactory<Key, BigDecimal, BigDecimal> getReducerFactory() {
        return new BigDecimalMaxReducerFactory<Key>();
    }

    /**
     * Maximum CombinerFactory for type {@link java.math.BigDecimal}
     *
     * @param <Key> the key type
     */
    static final class BigDecimalMaxCombinerFactory<Key>
            extends AbstractAggregationCombinerFactory<Key, BigDecimal, BigDecimal> {

        @Override
        public Combiner<BigDecimal, BigDecimal> newCombiner(Key key) {
            return new BigDecimalMaxCombiner();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_DECIMAL_MAX_COMBINER_FACTORY;
        }
    }

    /**
     * Maximum ReducerFactory for type {@link java.math.BigDecimal}
     *
     * @param <Key> the key type
     */
    static final class BigDecimalMaxReducerFactory<Key>
            extends AbstractAggregationReducerFactory<Key, BigDecimal, BigDecimal> {

        @Override
        public Reducer<BigDecimal, BigDecimal> newReducer(Key key) {
            return new BigDecimalMaxReducer();
        }

        @Override
        public int getId() {
            return AggregationsDataSerializerHook.BIG_DECIMAL_MAX_REDUCER_FACTORY;
        }
    }

    /**
     * Maximum Combiner for type {@link java.math.BigDecimal}
     */
    private static final class BigDecimalMaxCombiner
            extends Combiner<BigDecimal, BigDecimal> {

        private BigDecimal max;

        @Override
        public void combine(BigDecimal value) {
            max = max == null ? value : value.max(max);
        }

        @Override
        public BigDecimal finalizeChunk() {
            return max;
        }

        @Override
        public void reset() {
            max = null;
        }
    }

    /**
     * Maximum Reducer for type {@link java.math.BigDecimal}
     */
    private static final class BigDecimalMaxReducer
            extends Reducer<BigDecimal, BigDecimal> {

        private volatile BigDecimal max;

        @Override
        public void reduce(BigDecimal value) {
            max = max == null ? value : value.max(max);
        }

        @Override
        public BigDecimal finalizeReduce() {
            return max;
        }
    }
}
