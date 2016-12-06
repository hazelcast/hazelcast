/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation;

import com.hazelcast.aggregation.impl.BigDecimalAverageAggregator;
import com.hazelcast.aggregation.impl.BigDecimalSumAggregator;
import com.hazelcast.aggregation.impl.BigIntegerAverageAggregator;
import com.hazelcast.aggregation.impl.BigIntegerSumAggregator;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.DistinctValuesAggregator;
import com.hazelcast.aggregation.impl.DoubleAverageAggregator;
import com.hazelcast.aggregation.impl.DoubleSumAggregator;
import com.hazelcast.aggregation.impl.FixedSumAggregator;
import com.hazelcast.aggregation.impl.FloatingPointSumAggregator;
import com.hazelcast.aggregation.impl.IntegerAverageAggregator;
import com.hazelcast.aggregation.impl.IntegerSumAggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.aggregation.impl.NumberAverageAggregator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;

/**
 * A utility class to create basic {@link com.hazelcast.aggregation.Aggregator} instances.
 * <p>
 * Min/Max/Average aggregators are type specific, so an integerAvg() aggregator expects all elements to be integers.
 * There is no conversion executed while accumulating, so if there is any other type met an exception will be thrown.
 * <p>
 * In order to operate on a generic Number type use the fixedPointSum(), floatingPointSum() and numberAvg() aggregators.
 * All of them will convert the given number to either Long or Double during the accumulation phase.
 * It will result in a lot of allocations since each number has to be converted, but it enables the user
 * to operate on the whole family of numbers. It is especially useful if the numbers given to the aggregators
 * may not be of one type only.
 * <p>
 * The attributePath given in the factory method allows the aggregator to operate on the value extracted by navigating
 * to the given attributePath on each object that has been returned from a query.
 * The attribute path may be simple, e.g. "name", or nested "address.city".
 *
 * @since 3.8
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public final class Aggregators {

    private Aggregators() {
    }

    public static <I> Aggregator<I, Long> count() {
        return new CountAggregator<I>();
    }

    public static <I> Aggregator<I, Long> count(String attributePath) {
        return new CountAggregator<I>(attributePath);
    }

    public static <I, R> Aggregator<I, Set<R>> distinct() {
        return new DistinctValuesAggregator<I, R>();
    }

    public static <I, R> Aggregator<I, Set<R>> distinct(String attributePath) {
        return new DistinctValuesAggregator<I, R>(attributePath);
    }

    //
    // average aggregators
    //
    public static <I> Aggregator<I, BigDecimal> bigDecimalAvg() {
        return new BigDecimalAverageAggregator<I>();
    }

    public static <I> Aggregator<I, BigDecimal> bigDecimalAvg(String attributePath) {
        return new BigDecimalAverageAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, BigDecimal> bigIntegerAvg() {
        return new BigIntegerAverageAggregator<I>();
    }

    public static <I> Aggregator<I, BigDecimal> bigIntegerAvg(String attributePath) {
        return new BigIntegerAverageAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> doubleAvg() {
        return new DoubleAverageAggregator<I>();
    }

    public static <I> Aggregator<I, Double> doubleAvg(String attributePath) {
        return new DoubleAverageAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> integerAvg() {
        return new IntegerAverageAggregator<I>();
    }

    public static <I> Aggregator<I, Double> integerAvg(String attributePath) {
        return new IntegerAverageAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> longAvg() {
        return new LongAverageAggregator<I>();
    }

    public static <I> Aggregator<I, Double> longAvg(String attributePath) {
        return new LongAverageAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> numberAvg() {
        return new NumberAverageAggregator<I>();
    }

    public static <I> Aggregator<I, Double> numberAvg(String attributePath) {
        return new NumberAverageAggregator<I>(attributePath);
    }

    //
    // max aggregators
    //
    public static <I> Aggregator<I, BigDecimal> bigDecimalMax() {
        return new MaxAggregator<I, BigDecimal>();
    }

    public static <I> Aggregator<I, BigDecimal> bigDecimalMax(String attributePath) {
        return new MaxAggregator<I, BigDecimal>(attributePath);
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerMax() {
        return new MaxAggregator<I, BigInteger>();
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerMax(String attributePath) {
        return new MaxAggregator<I, BigInteger>(attributePath);
    }

    public static <I> Aggregator<I, Double> doubleMax() {
        return new MaxAggregator<I, Double>();
    }

    public static <I> Aggregator<I, Double> doubleMax(String attributePath) {
        return new MaxAggregator<I, Double>(attributePath);
    }

    public static <I> Aggregator<I, Integer> integerMax() {
        return new MaxAggregator<I, Integer>();
    }

    public static <I> Aggregator<I, Integer> integerMax(String attributePath) {
        return new MaxAggregator<I, Integer>(attributePath);
    }

    public static <I> Aggregator<I, Long> longMax() {
        return new MaxAggregator<I, Long>();
    }

    public static <I> Aggregator<I, Long> longMax(String attributePath) {
        return new MaxAggregator<I, Long>(attributePath);
    }

    public static <I, R extends Comparable> Aggregator<I, R> comparableMax() {
        return new MaxAggregator<I, R>();
    }

    public static <I, R extends Comparable> Aggregator<I, R> comparableMax(String attributePath) {
        return new MaxAggregator<I, R>(attributePath);
    }

    //
    // min aggregators
    //
    public static <I> Aggregator<I, BigDecimal> bigDecimalMin() {
        return new MinAggregator<I, BigDecimal>();
    }

    public static <I> Aggregator<I, BigDecimal> bigDecimalMin(String attributePath) {
        return new MinAggregator<I, BigDecimal>(attributePath);
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerMin() {
        return new MinAggregator<I, BigInteger>();
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerMin(String attributePath) {
        return new MinAggregator<I, BigInteger>(attributePath);
    }

    public static <I> Aggregator<I, Double> doubleMin() {
        return new MinAggregator<I, Double>();
    }

    public static <I> Aggregator<I, Double> doubleMin(String attributePath) {
        return new MinAggregator<I, Double>(attributePath);
    }

    public static <I> Aggregator<I, Integer> integerMin() {
        return new MinAggregator<I, Integer>();
    }

    public static <I> Aggregator<I, Integer> integerMin(String attributePath) {
        return new MinAggregator<I, Integer>(attributePath);
    }

    public static <I> Aggregator<I, Long> longMin() {
        return new MinAggregator<I, Long>();
    }

    public static <I> Aggregator<I, Long> longMin(String attributePath) {
        return new MinAggregator<I, Long>(attributePath);
    }

    public static <I, R extends Comparable> Aggregator<I, R> comparableMin() {
        return new MinAggregator<I, R>();
    }

    public static <I, R extends Comparable> Aggregator<I, R> comparableMin(String attributePath) {
        return new MinAggregator<I, R>(attributePath);
    }

    //
    // sum aggregators
    //
    public static <I> Aggregator<I, BigDecimal> bigDecimalSum() {
        return new BigDecimalSumAggregator<I>();
    }

    public static <I> Aggregator<I, BigDecimal> bigDecimalSum(String attributePath) {
        return new BigDecimalSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerSum() {
        return new BigIntegerSumAggregator<I>();
    }

    public static <I> Aggregator<I, BigInteger> bigIntegerSum(String attributePath) {
        return new BigIntegerSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> doubleSum() {
        return new DoubleSumAggregator<I>();
    }

    public static <I> Aggregator<I, Double> doubleSum(String attributePath) {
        return new DoubleSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Long> integerSum() {
        return new IntegerSumAggregator<I>();
    }

    public static <I> Aggregator<I, Long> integerSum(String attributePath) {
        return new IntegerSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Long> longSum() {
        return new LongSumAggregator<I>();
    }

    public static <I> Aggregator<I, Long> longSum(String attributePath) {
        return new LongSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Long> fixedPointSum() {
        return new FixedSumAggregator<I>();
    }

    public static <I> Aggregator<I, Long> fixedPointSum(String attributePath) {
        return new FixedSumAggregator<I>(attributePath);
    }

    public static <I> Aggregator<I, Double> floatingPointSum() {
        return new FloatingPointSumAggregator<I>();
    }

    public static <I> Aggregator<I, Double> floatingPointSum(String attributePath) {
        return new FloatingPointSumAggregator<I>(attributePath);
    }
}
