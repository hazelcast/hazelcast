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

import com.hazelcast.core.IMap;

import com.hazelcast.mapreduce.aggregation.impl.MinMaxTuple;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MinMaxAggregationTest
        extends AbstractAggregationTest {

    @Test
    public void testBigDecimalMinMax()
            throws Exception {

        BigDecimal[] values = buildPlainValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        }, BigDecimal.class);

        BigDecimal minExpectation = BigDecimal.ZERO;
        BigDecimal maxExpectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            BigDecimal value = values[i];
            minExpectation = i == 0 ? value : minExpectation.min(value);
            maxExpectation = i == 0 ? value : maxExpectation.max(value);
        }

        Aggregation<String, BigDecimal, MinMaxTuple<BigDecimal>> aggregation = Aggregations.bigDecimalMinMax();
        MinMaxTuple<BigDecimal> result = testMinMax(values, aggregation);
        assertEquals(minExpectation, result.getMinimum());
        assertEquals(maxExpectation, result.getMaximum());
    }

    @Test
    public void testBigIntegerMinMax()
            throws Exception {

        BigInteger[] values = buildPlainValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        }, BigInteger.class);

        BigInteger minExpectation = BigInteger.ZERO;
        BigInteger maxExpectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            BigInteger value = values[i];
            minExpectation = i == 0 ? value : minExpectation.min(value);
            maxExpectation = i == 0 ? value : maxExpectation.max(value);
        }

        Aggregation<String, BigInteger, MinMaxTuple<BigInteger>> aggregation = Aggregations.bigIntegerMinMax();
        MinMaxTuple<BigInteger> result = testMinMax(values, aggregation);
        assertEquals(minExpectation, result.getMinimum());
        assertEquals(maxExpectation, result.getMaximum());
    }

    @Test
    public void testDoubleMinMax()
            throws Exception {

        Double[] values = buildPlainValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        }, Double.class);

        double minExpectation = Double.MAX_VALUE;
        double maxExpectation = Double.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            double value = values[i];
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Double, MinMaxTuple<Double>> aggregation = Aggregations.doubleMinMax();
        MinMaxTuple<Double> result = testMinMax(values, aggregation);
        assertEquals(minExpectation, result.getMinimum(), 0.0);
        assertEquals(maxExpectation, result.getMaximum(), 0.0);
    }

    @Test
    public void testIntegerMinMax()
            throws Exception {

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        int minExpectation = Integer.MAX_VALUE;
        int maxExpectation = Integer.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            int value = values[i];
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Integer, MinMaxTuple<Integer>> aggregation = Aggregations.integerMinMax();
        MinMaxTuple<Integer> result = testMinMax(values, aggregation);
        assertEquals(minExpectation, (int)result.getMinimum());
        assertEquals(maxExpectation, (int)result.getMaximum());
    }

    @Test
    public void testLongMinMax()
            throws Exception {

        Long[] values = buildPlainValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        }, Long.class);

        long minExpectation = Long.MAX_VALUE;
        long maxExpectation = Long.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Long, MinMaxTuple<Long>> aggregation = Aggregations.longMinMax();
        MinMaxTuple<Long> result = testMinMax(values, aggregation);
        assertEquals(minExpectation, (long)result.getMinimum());
        assertEquals(maxExpectation, (long)result.getMaximum());
    }

    @Test
    public void testBigDecimalMinMaxWithExtractor()
            throws Exception {

        Value<BigDecimal>[] values = buildValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        });

        BigDecimal minExpectation = BigDecimal.ZERO;
        BigDecimal maxExpectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            Value<BigDecimal> value = values[i];
            minExpectation = i == 0 ? value.value : minExpectation.min(value.value);
            maxExpectation = i == 0 ? value.value : maxExpectation.max(value.value);
        }

        Aggregation<String, BigDecimal, MinMaxTuple<BigDecimal>> aggregation = Aggregations.bigDecimalMinMax();
        MinMaxTuple<BigDecimal> result = testMinMaxWithExtractor(values, aggregation);
        assertEquals(minExpectation, result.getMinimum());
        assertEquals(maxExpectation, result.getMaximum());
    }

    @Test
    public void testBigIntegerMinMaxWithExtractor()
            throws Exception {

        Value<BigInteger>[] values = buildValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        });

        BigInteger minExpectation = BigInteger.ZERO;
        BigInteger maxExpectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            Value<BigInteger> value = values[i];
            minExpectation = i == 0 ? value.value : minExpectation.min(value.value);
            maxExpectation = i == 0 ? value.value : maxExpectation.max(value.value);
        }

        Aggregation<String, BigInteger, MinMaxTuple<BigInteger>> aggregation = Aggregations.bigIntegerMinMax();
        MinMaxTuple<BigInteger> result = testMinMaxWithExtractor(values, aggregation);
        assertEquals(minExpectation, result.getMinimum());
        assertEquals(maxExpectation, result.getMaximum());
    }

    @Test
    public void testDoubleMinMaxWithExtractor()
            throws Exception {

        Value<Double>[] values = buildValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        });

        double minExpectation = Double.MAX_VALUE;
        double maxExpectation = Double.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            double value = values[i].value;
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Double, MinMaxTuple<Double>> aggregation = Aggregations.doubleMinMax();
        MinMaxTuple<Double> result = testMinMaxWithExtractor(values, aggregation);
        assertEquals(minExpectation, result.getMinimum(), 0.0);
        assertEquals(maxExpectation, result.getMaximum(), 0.0);
    }

    @Test
    public void testIntegerMinMaxWithExtractor()
            throws Exception {

        Value<Integer>[] values = buildValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        });

        int minExpectation = Integer.MAX_VALUE;
        int maxExpectation = Integer.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            int value = values[i].value;
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Integer, MinMaxTuple<Integer>> aggregation = Aggregations.integerMinMax();
        MinMaxTuple<Integer> result = testMinMaxWithExtractor(values, aggregation);
        assertEquals(minExpectation, (int)result.getMinimum());
        assertEquals(maxExpectation, (int)result.getMaximum());
    }

    @Test
    public void testLongMinMaxWithExtractor()
            throws Exception {

        Value<Long>[] values = buildValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        });

        long minExpectation = Long.MAX_VALUE;
        long maxExpectation = Long.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            long value = values[i].value;
            minExpectation = Math.min(minExpectation, value);
            maxExpectation = Math.max(maxExpectation, value);
        }

        Aggregation<String, Long, MinMaxTuple<Long>> aggregation = Aggregations.longMinMax();
        MinMaxTuple<Long> result = testMinMaxWithExtractor(values, aggregation);
        assertEquals(minExpectation, (long)result.getMinimum());
        assertEquals(maxExpectation, (long)result.getMaximum());
    }

    private <T, R> R testMinMax(T[] values, Aggregation<String, T, R> aggregation)
            throws Exception {

        String mapName = randomMapName();
        IMap<String, T> map = HAZELCAST_INSTANCE.getMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, T, T> supplier = Supplier.all();
        return map.aggregate(supplier, aggregation);
    }

    private <T, R> R testMinMaxWithExtractor(Value<T>[] values, Aggregation<String, T, R> aggregation)
            throws Exception {

        String mapName = randomMapName();
        IMap<String, Value<T>> map = HAZELCAST_INSTANCE.getMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, Value<T>, T> supplier = Supplier.all(new ValuePropertyExtractor<T>());
        return map.aggregate(supplier, aggregation);
    }
}
