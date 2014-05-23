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

package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.aggregation.impl.AggType;
import com.hazelcast.mapreduce.aggregation.impl.BigDecimalAvgAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigDecimalMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigDecimalMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigDecimalSumAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigIntegerAvgAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigIntegerMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigIntegerMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.BigIntegerSumAggregation;
import com.hazelcast.mapreduce.aggregation.impl.ComparableMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.ComparableMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.CountAggregation;
import com.hazelcast.mapreduce.aggregation.impl.DoubleAvgAggregation;
import com.hazelcast.mapreduce.aggregation.impl.DoubleMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.DoubleMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.DoubleSumAggregation;
import com.hazelcast.mapreduce.aggregation.impl.IntegerAvgAggregation;
import com.hazelcast.mapreduce.aggregation.impl.IntegerMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.IntegerMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.IntegerSumAggregation;
import com.hazelcast.mapreduce.aggregation.impl.LongAvgAggregation;
import com.hazelcast.mapreduce.aggregation.impl.LongMaxAggregation;
import com.hazelcast.mapreduce.aggregation.impl.LongMinAggregation;
import com.hazelcast.mapreduce.aggregation.impl.LongSumAggregation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class Aggregations {

    public static Aggregation<Object, Object, Long> count() {
        return new AggregationAdapter(new CountAggregation<Object, Object>());
    }

    public static <Key, Value> Aggregation<Key, Value, Integer> integerAvg() {
        return new AggregationAdapter(new IntegerAvgAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Integer, Integer> integerSum() {
        return new AggregationAdapter(new IntegerSumAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Integer, Integer> integerMin() {
        return new AggregationAdapter(new IntegerMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Integer, Integer> integerMax() {
        return new AggregationAdapter(new IntegerMaxAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Long, Long> longAvg() {
        return new AggregationAdapter(new LongAvgAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Long, Long> longSum() {
        return new AggregationAdapter(new LongSumAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Long, Long> longMin() {
        return new AggregationAdapter(new LongMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Long, Long> longMax() {
        return new AggregationAdapter(new LongMaxAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Double, Double> doubleAvg() {
        return new AggregationAdapter(new DoubleAvgAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Double, Double> doubleSum() {
        return new AggregationAdapter(new DoubleSumAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Double, Double> doubleMin() {
        return new AggregationAdapter(new DoubleMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Double, Double> doubleMax() {
        return new AggregationAdapter(new DoubleMaxAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalAvg() {
        return new AggregationAdapter(new BigDecimalAvgAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalSum() {
        return new AggregationAdapter(new BigDecimalSumAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalMin() {
        return new AggregationAdapter(new BigDecimalMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalMax() {
        return new AggregationAdapter(new BigDecimalMaxAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerAvg() {
        return new AggregationAdapter(new BigIntegerAvgAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerSum() {
        return new AggregationAdapter(new BigIntegerSumAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerMin() {
        return new AggregationAdapter(new BigIntegerMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerMax() {
        return new AggregationAdapter(new BigIntegerMaxAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Comparable, Comparable> comparableMin() {
        return new AggregationAdapter(new ComparableMinAggregation<Key, Value>());
    }

    public static <Key, Value> Aggregation<Key, Comparable, Comparable> comparableMax() {
        return new AggregationAdapter(new ComparableMaxAggregation<Key, Value>());
    }

    private static final class AggregationAdapter<Key, Supplied, Result>
            implements Aggregation<Key, Supplied, Result> {

        private final AggType internalAggregationType;

        private AggregationAdapter(AggType internalAggregationType) {
            this.internalAggregationType = internalAggregationType;
        }

        @Override
        public Collator<Map.Entry, Result> getCollator() {
            return internalAggregationType.getCollator();
        }

        @Override
        public Mapper getMapper(Supplier<Key, ?, Supplied> supplier) {
            return internalAggregationType.getMapper(supplier);
        }

        @Override
        public CombinerFactory getCombinerFactory() {
            return internalAggregationType.getCombinerFactory();
        }

        @Override
        public ReducerFactory getReducerFactory() {
            return internalAggregationType.getReducerFactory();
        }
    }
}
