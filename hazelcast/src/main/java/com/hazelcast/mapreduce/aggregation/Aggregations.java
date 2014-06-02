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
import com.hazelcast.mapreduce.aggregation.impl.DistinctValuesAggregation;
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
import com.hazelcast.spi.annotation.Beta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to access the Hazelcast predefined set of aggregations in a type-safe
 * way.
 * @since 3.3
 */
@Beta
public final class Aggregations {

    private Aggregations() {
    }

    /**
     * Returns an aggregation for counting all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT COUNT(*) FROM x</pre>
     *
     * @param <Key> the input key type
     * @return the count of all supplied elements
     */
    public static <Key> Aggregation<Key, Object, Long> count() {
        return new AggregationAdapter(new CountAggregation<Object, Object>());
    }

    /**
     * Returns an aggregation for selecting all distinct values.<br/>
     * This aggregation is similar to <pre>SELECT DISTINCT * FROM x</pre>
     *
     * @param <Key>          the input key type
     * @param <Value>        the supplied value type
     * @param <DistinctType> the type of all distinct values
     * @return a {@link java.util.Set} containing all distinct values
     */
    public static <Key, Value, DistinctType> Aggregation<Key, Value, Set<DistinctType>> distinctValues() {
        AggType<Key, Value, Integer, DistinctType, Set<DistinctType>, Set<DistinctType>, Set<DistinctType>> aggType;
        aggType = new DistinctValuesAggregation<Key, Value, DistinctType>();
        return new AggregationAdapter<Key, Value, Set<DistinctType>>(aggType);
    }

    /**
     * Returns an aggregation to calculate the integer average of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT AVG(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the average over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Value, Integer> integerAvg() {
        return new AggregationAdapter(new IntegerAvgAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the integer sum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT SUM(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the sum over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Integer, Integer> integerSum() {
        return new AggregationAdapter(new IntegerSumAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the integer minimum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Integer, Integer> integerMin() {
        return new AggregationAdapter(new IntegerMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the integer maximum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Integer, Integer> integerMax() {
        return new AggregationAdapter(new IntegerMaxAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the long average of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT AVG(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the average over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Long, Long> longAvg() {
        return new AggregationAdapter(new LongAvgAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the long sum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT SUM(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the sum over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Long, Long> longSum() {
        return new AggregationAdapter(new LongSumAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the long minimum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Long, Long> longMin() {
        return new AggregationAdapter(new LongMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the long maximum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Long, Long> longMax() {
        return new AggregationAdapter(new LongMaxAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the double average of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT AVG(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the average over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Double, Double> doubleAvg() {
        return new AggregationAdapter(new DoubleAvgAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the double sum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT SUM(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the sum over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Double, Double> doubleSum() {
        return new AggregationAdapter(new DoubleSumAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the double minimum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Double, Double> doubleMin() {
        return new AggregationAdapter(new DoubleMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the double maximum of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Double, Double> doubleMax() {
        return new AggregationAdapter(new DoubleMaxAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the {@link java.math.BigDecimal} average
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT AVG(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the average over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalAvg() {
        return new AggregationAdapter(new BigDecimalAvgAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the {@link java.math.BigDecimal} sum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT SUM(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the sum over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalSum() {
        return new AggregationAdapter(new BigDecimalSumAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the {@link java.math.BigDecimal} minimum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalMin() {
        return new AggregationAdapter(new BigDecimalMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the {@link java.math.BigDecimal} maximum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigDecimal, BigDecimal> bigDecimalMax() {
        return new AggregationAdapter(new BigDecimalMaxAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the {@link java.math.BigInteger} average
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT AVG(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the average over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerAvg() {
        return new AggregationAdapter(new BigIntegerAvgAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to calculate the {@link java.math.BigInteger} sum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT SUM(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the sum over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerSum() {
        return new AggregationAdapter(new BigIntegerSumAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the {@link java.math.BigInteger} minimum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerMin() {
        return new AggregationAdapter(new BigIntegerMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the {@link java.math.BigInteger} maximum
     * of all supplied values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, BigInteger, BigInteger> bigIntegerMax() {
        return new AggregationAdapter(new BigIntegerMaxAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the minimum value of all supplied
     * {@link java.lang.Comparable} implementing values.<br/>
     * This aggregation is similar to <pre>SELECT MIN(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the minimum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Comparable, Comparable> comparableMin() {
        return new AggregationAdapter(new ComparableMinAggregation<Key, Value>());
    }

    /**
     * Returns an aggregation to find the maximum value of all supplied
     * {@link java.lang.Comparable} implementing values.<br/>
     * This aggregation is similar to <pre>SELECT MAX(value) FROM x</pre>
     *
     * @param <Key>   the input key type
     * @param <Value> the supplied value type
     * @return the maximum value over all supplied values
     */
    public static <Key, Value> Aggregation<Key, Comparable, Comparable> comparableMax() {
        return new AggregationAdapter(new ComparableMaxAggregation<Key, Value>());
    }

    /**
     * Adapter class from internal {@link com.hazelcast.mapreduce.aggregation.impl.AggType}
     * to public API {@link com.hazelcast.mapreduce.aggregation.Aggregation}s. It is used to
     * make the internal aggregations implementation more type-safe but exporting only necessary
     * types to the public API.
     *
     * @param <Key>      the input key type
     * @param <Supplied> the supplied value type
     * @param <Result>   the result value type
     */
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
