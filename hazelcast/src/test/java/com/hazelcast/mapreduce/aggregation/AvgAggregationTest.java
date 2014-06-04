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
public class AvgAggregationTest
        extends AbstractAggregationTest {

    @Test
    public void testBigDecimalAvg()
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
        expectation = expectation.divide(BigDecimal.valueOf(values.length));

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalAvg();
        BigDecimal result = testAvg(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerAvg()
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
        expectation = expectation.divide(BigInteger.valueOf(values.length));

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerAvg();
        BigInteger result = testAvg(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleAvg()
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
        expectation = expectation / values.length;

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleAvg();
        double result = testAvg(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerAvg()
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
        expectation = (int) ((double) expectation / values.length);

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerAvg();
        int result = testAvg(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongAvg()
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
        expectation = (long) ((double) expectation / values.length);

        Aggregation<String, Long, Long> aggregation = Aggregations.longAvg();
        long result = testAvg(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigDecimalAvgWithExtractor()
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
        expectation = expectation.divide(BigDecimal.valueOf(values.length));

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalAvg();
        BigDecimal result = testAvgWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerAvgWithExtractor()
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
        expectation = expectation.divide(BigInteger.valueOf(values.length));

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerAvg();
        BigInteger result = testAvgWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleAvgWithExtractor()
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
        expectation = expectation / values.length;

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleAvg();
        double result = testAvgWithExtractor(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerAvgWithExtractor()
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
        expectation = (int) ((double) expectation / values.length);

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerAvg();
        int result = testAvgWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongAvgWithExtractor()
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
        expectation = (long) ((double) expectation / values.length);

        Aggregation<String, Long, Long> aggregation = Aggregations.longAvg();
        long result = testAvgWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    private <T, R> R testAvg(T[] values, Aggregation<String, T, R> aggregation)
            throws Exception {

        String mapName = randomMapName();
        IMap<String, T> map = HAZELCAST_INSTANCE.getMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, T, T> supplier = Supplier.all();
        return map.aggregate(supplier, aggregation);
    }

    private <T, R> R testAvgWithExtractor(Value<T>[] values, Aggregation<String, T, R> aggregation)
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
