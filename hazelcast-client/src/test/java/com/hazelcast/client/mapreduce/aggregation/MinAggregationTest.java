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

package com.hazelcast.client.mapreduce.aggregation;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MinAggregationTest  extends AbstractAggregationTest {
    @Test
    public void testBigDecimalMin() throws Exception {
        BigDecimal[] values = buildPlainValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        }, BigDecimal.class);

        BigDecimal expectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            BigDecimal value = values[i];
            expectation = i == 0 ? value : expectation.min(value);
        }

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalMin();
        BigDecimal result = testMin(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerMin() throws Exception {
        BigInteger[] values = buildPlainValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        }, BigInteger.class);

        BigInteger expectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            BigInteger value = values[i];
            expectation = i == 0 ? value : expectation.min(value);
        }

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerMin();
        BigInteger result = testMin(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleMin() throws Exception {
        Double[] values = buildPlainValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        }, Double.class);

        double expectation = Double.MAX_VALUE;
        for (Double value : values) {
            if (value < expectation) {
                expectation = value;
            }
        }

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleMin();
        double result = testMin(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerMin() throws Exception {
        Integer[] values = buildPlainValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        }, Integer.class);

        int expectation = Integer.MAX_VALUE;
        for (Integer value : values) {
            if (value < expectation) {
                expectation = value;
            }
        }

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerMin();
        int result = testMin(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongMin() throws Exception {
        Long[] values = buildPlainValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        }, Long.class);

        long expectation = Long.MAX_VALUE;
        for (Long value : values) {
            if (value < expectation) {
                expectation = value;
            }
        }

        Aggregation<String, Long, Long> aggregation = Aggregations.longMin();
        long result = testMin(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigDecimalMinWithExtractor() throws Exception {
        Value<BigDecimal>[] values = buildValues(new ValueProvider<BigDecimal>() {
            @Override
            public BigDecimal provideRandom(Random random) {
                return BigDecimal.valueOf(10000.0D + random(1000, 2000));
            }
        });

        BigDecimal expectation = BigDecimal.ZERO;
        for (int i = 0; i < values.length; i++) {
            Value<BigDecimal> value = values[i];
            expectation = i == 0 ? value.value : expectation.min(value.value);
        }

        Aggregation<String, BigDecimal, BigDecimal> aggregation = Aggregations.bigDecimalMin();
        BigDecimal result = testMinWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testBigIntegerMinWithExtractor() throws Exception {
        Value<BigInteger>[] values = buildValues(new ValueProvider<BigInteger>() {
            @Override
            public BigInteger provideRandom(Random random) {
                return BigInteger.valueOf(10000L + random(1000, 2000));
            }
        });

        BigInteger expectation = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            Value<BigInteger> value = values[i];
            expectation = i == 0 ? value.value : expectation.min(value.value);
        }

        Aggregation<String, BigInteger, BigInteger> aggregation = Aggregations.bigIntegerMin();
        BigInteger result = testMinWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testDoubleMinWithExtractor() throws Exception {

        Value<Double>[] values = buildValues(new ValueProvider<Double>() {
            @Override
            public Double provideRandom(Random random) {
                return 10000.0D + random(1000, 2000);
            }
        });

        double expectation = Double.MAX_VALUE;
        for (Value<Double> value : values) {
            if (value.value < expectation) {
                expectation = value.value;
            }
        }

        Aggregation<String, Double, Double> aggregation = Aggregations.doubleMin();
        double result = testMinWithExtractor(values, aggregation);
        assertEquals(expectation, result, 0.0);
    }

    @Test
    public void testIntegerMinWithExtractor() throws Exception {
        Value<Integer>[] values = buildValues(new ValueProvider<Integer>() {
            @Override
            public Integer provideRandom(Random random) {
                return random(1000, 2000);
            }
        });

        int expectation = Integer.MAX_VALUE;
        for (Value<Integer> value : values) {
            if (value.value < expectation) {
                expectation = value.value;
            }
        }

        Aggregation<String, Integer, Integer> aggregation = Aggregations.integerMin();
        int result = testMinWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    @Test
    public void testLongMinWithExtractor() throws Exception {
        Value<Long>[] values = buildValues(new ValueProvider<Long>() {
            @Override
            public Long provideRandom(Random random) {
                return 10000L + random(1000, 2000);
            }
        });

        long expectation = Long.MAX_VALUE;
        for (Value<Long> value : values) {
            if (value.value < expectation) {
                expectation = value.value;
            }
        }

        Aggregation<String, Long, Long> aggregation = Aggregations.longMin();
        long result = testMinWithExtractor(values, aggregation);
        assertEquals(expectation, result);
    }

    private <T, R> R testMin(T[] values, Aggregation<String, T, R> aggregation) throws Exception {
        String mapName = randomMapName();
        IMap<String, T> map = HAZELCAST_INSTANCE.getMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, T, T> supplier = Supplier.all();
        return map.aggregate(supplier, aggregation);
    }

    private <T, R> R testMinWithExtractor(Value<T>[] values, Aggregation<String, T, R> aggregation) throws Exception {
        String mapName = randomMapName();
        IMap<String, Value<T>> map = HAZELCAST_INSTANCE.getMap(mapName);

        for (int i = 0; i < values.length; i++) {
            map.put("key-" + i, values[i]);
        }

        Supplier<String, Value<T>, T> supplier = Supplier.all(new ValuePropertyExtractor<T>());
        return map.aggregate(supplier, aggregation);
    }
}
