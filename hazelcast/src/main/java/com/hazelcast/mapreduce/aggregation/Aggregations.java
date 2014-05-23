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

import com.hazelcast.mapreduce.aggregation.impl.AvgTuple;
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

public class Aggregations {

    public static Aggregation<Object, Object, Object, Long, Long, Long, Long> count() {
        return new CountAggregation<Object, Object>();
    }

    //CHECKSTYLE:OFF
    public static <Key, Value> Aggregation<Key, Value, Key, Integer, //
            AvgTuple<Integer, Integer>, AvgTuple<Integer, Integer>, Integer> integerAvg() {

        return new IntegerAvgAggregation<Key, Value>();
    }
    //CHECKSTYLE:ON

    public static <Key, Value> Aggregation<Key, Value, Key, Integer, Integer, Integer, Integer> integerSum() {
        return new IntegerSumAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Integer, Integer, Integer, Integer> integerMin() {
        return new IntegerMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Integer, Integer, Integer, Integer> integerMax() {
        return new IntegerMaxAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Long, AvgTuple<Long, Long>, AvgTuple<Long, Long>, Long> longAvg() {
        return new LongAvgAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Long, Long, Long, Long> longSum() {
        return new LongSumAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Long, Long, Long, Long> longMin() {
        return new LongMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Long, Long, Long, Long> longMax() {
        return new LongMaxAggregation<Key, Value>();
    }

    //CHECKSTYLE:OFF
    public static <Key, Value> Aggregation<Key, Value, Key, Double, //
            AvgTuple<Long, Double>, AvgTuple<Long, Double>, Double> doubleAvg() {

        return new DoubleAvgAggregation<Key, Value>();
    }
    //CHECKSTYLE:ON

    public static <Key, Value> Aggregation<Key, Value, Key, Double, Double, Double, Double> doubleSum() {
        return new DoubleSumAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Double, Double, Double, Double> doubleMin() {
        return new DoubleMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Double, Double, Double, Double> doubleMax() {
        return new DoubleMaxAggregation<Key, Value>();
    }

    //CHECKSTYLE:OFF
    public static <Key, Value> Aggregation<Key, Value, Key, BigDecimal, //
            AvgTuple<Long, BigDecimal>, AvgTuple<Long, BigDecimal>, BigDecimal> bigDecimalAvg() {

        return new BigDecimalAvgAggregation<Key, Value>();
    }
    //CHECKSTYLE:ON

    public static <Key, Value> Aggregation<Key, Value, Key, BigDecimal, BigDecimal, BigDecimal, BigDecimal> bigDecimalSum() {
        return new BigDecimalSumAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, BigDecimal, BigDecimal, BigDecimal, BigDecimal> bigDecimalMin() {
        return new BigDecimalMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, BigDecimal, BigDecimal, BigDecimal, BigDecimal> bigDecimalMax() {
        return new BigDecimalMaxAggregation<Key, Value>();
    }

    //CHECKSTYLE:OFF
    public static <Key, Value> Aggregation<Key, Value, Key, BigInteger, //
            AvgTuple<Long, BigInteger>, AvgTuple<Long, BigInteger>, BigInteger> bigIntegerAvg() {

        return new BigIntegerAvgAggregation<Key, Value>();
    }
    //CHECKSTYLE:ON

    public static <Key, Value> Aggregation<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> bigIntegerSum() {
        return new BigIntegerSumAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> bigIntegerMin() {
        return new BigIntegerMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, BigInteger, BigInteger, BigInteger, BigInteger> bigIntegerMax() {
        return new BigIntegerMaxAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Comparable, Comparable, Comparable, Comparable> comparableMin() {
        return new ComparableMinAggregation<Key, Value>();
    }

    public static <Key, Value> Aggregation<Key, Value, Key, Comparable, Comparable, Comparable, Comparable> comparableMax() {
        return new ComparableMaxAggregation<Key, Value>();
    }
}
