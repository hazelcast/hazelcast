/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.aggregation.impl.MaxByAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.aggregation.impl.MinByAggregator;
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
 * <p>
 * If an aggregator does not accept null values pass a predicate to the aggregate call that will filter them out.
 * <p>
 * If the input value or the extracted value is a collection it won't be "unfolded" - so for example
 * count aggregation on "person.postalCodes" will return 1 for each input object and not the size of the collection.
 * In order to calculate the size of the collection use the [any] operator, e.g. "person.postalCodes[any]".
 *
 * @since 3.8
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public final class Aggregators {

    private Aggregators() {
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that counts the input values.
     * Accepts nulls as input values.
     * Aggregation result type Long.
     */
    public static <I> Aggregator<I, Long> count() {
        return new CountAggregator<I>();
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that counts the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Aggregation result type Long.
     */
    public static <I> Aggregator<I, Long> count(String attributePath) {
        return new CountAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @param <R> type of the return object.
     * @return an aggregator that calculates the distinct set of input values.
     * Accepts null input values.
     * Aggregation result type is a Set of R.
     */
    public static <I, R> Aggregator<I, Set<R>> distinct() {
        return new DistinctValuesAggregator<>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @param <R> type of the return object.
     * @return an aggregator that calculates the distinct set of input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Aggregation result type is a Set of R.
     */
    public static <I, R> Aggregator<I, Set<R>> distinct(String attributePath) {
        return new DistinctValuesAggregator<>(attributePath);
    }

    // ---------------------------------------------------------------------------------------------------------
    // average aggregators
    // ---------------------------------------------------------------------------------------------------------

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalAvg() {
        return new BigDecimalAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalAvg(String attributePath) {
        return new BigDecimalAverageAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigIntegerAvg() {
        return new BigIntegerAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigIntegerAvg(String attributePath) {
        return new BigIntegerAverageAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleAvg() {
        return new DoubleAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleAvg(String attributePath) {
        return new DoubleAverageAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> integerAvg() {
        return new IntegerAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> integerAvg(String attributePath) {
        return new IntegerAverageAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> longAvg() {
        return new LongAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> longAvg(String attributePath) {
        return new LongAverageAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values.
     * Does NOT accept null input values.
     * Accepts generic Number input values.
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> numberAvg() {
        return new NumberAverageAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the average of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts generic Number input values.
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> numberAvg(String attributePath) {
        return new NumberAverageAggregator<I>(attributePath);
    }

    // ---------------------------------------------------------------------------------------------------------
    // max aggregators
    // ---------------------------------------------------------------------------------------------------------

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalMax() {
        return new MaxAggregator<I, BigDecimal>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalMax(String attributePath) {
        return new MaxAggregator<I, BigDecimal>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerMax() {
        return new MaxAggregator<I, BigInteger>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values nor null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerMax(String attributePath) {
        return new MaxAggregator<I, BigInteger>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleMax() {
        return new MaxAggregator<I, Double>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleMax(String attributePath) {
        return new MaxAggregator<I, Double>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Integer.
     */
    public static <I> Aggregator<I, Integer> integerMax() {
        return new MaxAggregator<I, Integer>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Integer.
     */
    public static <I> Aggregator<I, Integer> integerMax(String attributePath) {
        return new MaxAggregator<I, Integer>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longMax() {
        return new MaxAggregator<I, Long>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longMax(String attributePath) {
        return new MaxAggregator<I, Long>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @param <R> type of the return object (subtype of Comparable)
     * @return an aggregator that calculates the max of the input values.
     * Accepts null input values.
     * Accepts generic Comparable input values.
     * Aggregation result type is R.
     */
    public static <I, R extends Comparable> Aggregator<I, R> comparableMax() {
        return new MaxAggregator<I, R>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @param <R> type of the return object (subtype of Comparable)
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts generic Comparable input values.
     * Aggregation result type is R.
     */
    public static <I, R extends Comparable> Aggregator<I, R> comparableMax(String attributePath) {
        return new MaxAggregator<I, R>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the max of the input values extracted from the given attributePath
     * and returns the input item containing the maximum value. If multiple items contain the maximum value,
     * any of them is returned.
     * Accepts null input values and null extracted values.
     * Accepts generic Comparable input values.
     */
    public static <I> Aggregator<I, I> maxBy(String attributePath) {
        return new MaxByAggregator<I>(attributePath);
    }

    // ---------------------------------------------------------------------------------------------------------
    // min aggregators
    // ---------------------------------------------------------------------------------------------------------

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalMin() {
        return new MinAggregator<I, BigDecimal>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalMin(String attributePath) {
        return new MinAggregator<I, BigDecimal>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerMin() {
        return new MinAggregator<I, BigInteger>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerMin(String attributePath) {
        return new MinAggregator<I, BigInteger>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleMin() {
        return new MinAggregator<I, Double>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleMin(String attributePath) {
        return new MinAggregator<I, Double>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Integer.
     */
    public static <I> Aggregator<I, Integer> integerMin() {
        return new MinAggregator<I, Integer>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Integer.
     */
    public static <I> Aggregator<I, Integer> integerMin(String attributePath) {
        return new MinAggregator<I, Integer>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values and null extracted values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longMin() {
        return new MinAggregator<I, Long>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longMin(String attributePath) {
        return new MinAggregator<I, Long>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @param <R> type of the return object (subtype of Comparable)
     * @return an aggregator that calculates the min of the input values.
     * Accepts null input values.
     * Accepts generic Comparable input values.
     * Aggregation result type is R.
     */
    public static <I, R extends Comparable> Aggregator<I, R> comparableMin() {
        return new MinAggregator<I, R>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @param <R> type of the return object (subtype of Comparable)
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath.
     * Accepts null input values and null extracted values.
     * Accepts generic Comparable input values.
     * Aggregation result type is R.
     */
    public static <I, R extends Comparable> Aggregator<I, R> comparableMin(String attributePath) {
        return new MinAggregator<I, R>(attributePath);
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the min of the input values extracted from the given attributePath
     * and returns the input item containing the minimum value. If multiple items contain the minimum value,
     * any of them is returned.
     * Accepts null input values and null extracted values.
     * Accepts generic Comparable input values.
     */
    public static <I> Aggregator<I, I> minBy(String attributePath) {
        return new MinByAggregator<I>(attributePath);
    }

    // ---------------------------------------------------------------------------------------------------------
    // sum aggregators
    // ---------------------------------------------------------------------------------------------------------

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalSum() {
        return new BigDecimalSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only BigDecimal input values.
     * Aggregation result type is BigDecimal.
     */
    public static <I> Aggregator<I, BigDecimal> bigDecimalSum(String attributePath) {
        return new BigDecimalSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerSum() {
        return new BigIntegerSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values nor null extracted values.
     * Accepts only BigInteger input values.
     * Aggregation result type is BigInteger.
     */
    public static <I> Aggregator<I, BigInteger> bigIntegerSum(String attributePath) {
        return new BigIntegerSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleSum() {
        return new DoubleSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values.
     * Accepts only Double input values (primitive and boxed).
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> doubleSum(String attributePath) {
        return new DoubleSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> integerSum() {
        return new IntegerSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values.
     * Accepts only Integer input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> integerSum(String attributePath) {
        return new IntegerSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longSum() {
        return new LongSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values.
     * Accepts only Long input values (primitive and boxed).
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> longSum(String attributePath) {
        return new LongSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts generic Number input values.
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> fixedPointSum() {
        return new FixedSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values.
     * Accepts generic Number input values.
     * Aggregation result type is Long.
     */
    public static <I> Aggregator<I, Long> fixedPointSum(String attributePath) {
        return new FixedSumAggregator<I>(attributePath);
    }

    /**
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values.
     * Does NOT accept null input values.
     * Accepts generic Number input values.
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> floatingPointSum() {
        return new FloatingPointSumAggregator<I>();
    }

    /**
     * @param attributePath the attribute path
     * @param <I> type of the input object.
     * @return an aggregator that calculates the sum of the input values extracted from the given attributePath.
     * Does NOT accept null input values.
     * Accepts generic Number input values.
     * Aggregation result type is Double.
     */
    public static <I> Aggregator<I, Double> floatingPointSum(String attributePath) {
        return new FloatingPointSumAggregator<I>(attributePath);
    }
}
