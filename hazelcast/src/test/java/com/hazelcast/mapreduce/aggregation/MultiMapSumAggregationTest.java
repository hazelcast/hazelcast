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

import com.hazelcast.core.MultiMap;
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
public class MultiMapSumAggregationTest
        extends AbstractAggregationTest {

    @Test
    public void testBigDecimalSum()
            throws Exception {

        BigDecimal[] values = buildPlainValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        }, BigDecimal.class);

        BigDecimal expectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            expectation = expectation.add(values[i]);
        }

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalSum();
        BigDecimal result = testSum(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerSum()
            throws Exception {

        BigInteger[] values = buildPlainValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        }, BigInteger.class);

        BigInteger expectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            expectation = expectation.add(values[i]);
        }

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerSum();
        BigInteger result = testSum(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleSum()
            throws Exception {

        Double[] values = buildPlainValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        }, Double.class);

        double expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i];
        }

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleSum();
        double result = testSum(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerSum()
            throws Exception {

        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        int expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i];
        }

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerSum();
        int result = testSum(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongSum()
            throws Exception {

        Long[] values = buildPlainValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        }, Long.class);

        long expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i];
        }

        Aggregation<String, Long, Long> aggregation = Aggregations.longSum();
        long result = testSum(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigDecimalSumWithExtractor()
            throws Exception {

        Value<BigDecimal>[] values = buildValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        });

        BigDecimal expectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            expectation = expectation.add(values[i].value);
        }

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalSum();
        BigDecimal result = testSumWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerSumWithExtractor()
            throws Exception {

        Value<BigInteger>[] values = buildValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        });

        BigInteger expectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            expectation = expectation.add(values[i].value);
        }

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerSum();
        BigInteger result = testSumWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleSumWithExtractor()
            throws Exception {

        Value<Double>[] values = buildValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        });

        double expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i].value;
        }

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleSum();
        double result = testSumWithExtractor(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerSumWithExtractor()
            throws Exception {

        Value<Integer>[] values = buildValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        });

        int expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i].value;
        }

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerSum();
        int result = testSumWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongSumWithExtractor()
            throws Exception {

        Value<Long>[] values = buildValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        });

        long expectation = 0;
        for (int i = 0; i < values.length; i++) {
            expectation += values[i].value;
        }

        Aggregation<String, Long, Long> aggregation = Aggregations.longSum();
        long result = testSumWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    private <T, R> R testSum(T[] values, Aggregation<String, T, R> aggregation)
            throws Exception {

        String mapName = randomMapName();
        MultiMap<String, T> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, T, T> supplier = Supplier.all();
        return map.aggregate(supplier, aggregation);
    }

    private <T, R> R testSumWithExtractor(Value<T>[] values, Aggregation<String, T, R> aggregation)
            throws Exception {

        String mapName = randomMapName();
        MultiMap<String, Value<T>> map = HAZELCAST_INSTANCE.getMultiMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, Value<T>, T> supplier = Supplier.all(new ValuePropertyExtractor<T>());
        return map.aggregate(supplier, aggregation);
    }
}
