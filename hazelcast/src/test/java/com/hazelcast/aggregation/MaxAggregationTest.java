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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static com.hazelcast.aggregation.TestSamples.sampleStrings;
import static com.hazelcast.aggregation.TestSamples.sampleValueContainers;
import static com.hazelcast.aggregation.ValueContainer.ValueType.BIG_DECIMAL;
import static com.hazelcast.aggregation.ValueContainer.ValueType.BIG_INTEGER;
import static com.hazelcast.aggregation.ValueContainer.ValueType.DOUBLE;
import static com.hazelcast.aggregation.ValueContainer.ValueType.INTEGER;
import static com.hazelcast.aggregation.ValueContainer.ValueType.LONG;
import static com.hazelcast.aggregation.ValueContainer.ValueType.STRING;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MaxAggregationTest {

    public static final double ERROR = 1e-8;

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalMax() {
        List<BigDecimal> values = sampleBigDecimals();
        Collections.sort(values);
        BigDecimal expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> aggregation = Aggregators.bigDecimalMax();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> resultAggregation = Aggregators.bigDecimalMax();
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_DECIMAL);
        Collections.sort(values);
        BigDecimal expectation = values.get(values.size() - 1).bigDecimal;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> aggregation = Aggregators.bigDecimalMax("bigDecimal");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> resultAggregation = Aggregators.bigDecimalMax("bigDecimal");
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerMax() {
        List<BigInteger> values = sampleBigIntegers();
        Collections.sort(values);
        BigInteger expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigInteger> aggregation = Aggregators.bigIntegerMax();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigInteger> resultAggregation = Aggregators.bigIntegerMax();
        resultAggregation.combine(aggregation);
        BigInteger result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_INTEGER);
        Collections.sort(values);
        BigInteger expectation = values.get(values.size() - 1).bigInteger;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigInteger> aggregation = Aggregators.bigIntegerMax("bigInteger");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigInteger> resultAggregation
                = Aggregators.bigIntegerMax("bigInteger");
        resultAggregation.combine(aggregation);
        BigInteger result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleMax() {
        List<Double> values = sampleDoubles();
        Collections.sort(values);
        double expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<Double, Double>, Double> aggregation = Aggregators.doubleMax();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Double, Double>, Double> resultAggregation = Aggregators.doubleMax();
        resultAggregation.combine(aggregation);
        Double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(DOUBLE);
        Collections.sort(values);
        double expectation = values.get(values.size() - 1).doubleValue;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.doubleMax("doubleValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<Double, Double>, Double> resultAggregation = Aggregators.doubleMax("doubleValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerMax() {
        List<Integer> values = sampleIntegers();
        Collections.sort(values);
        long expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<Integer, Integer>, Integer> aggregation = Aggregators.integerMax();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Integer, Integer>, Integer> resultAggregation = Aggregators.integerMax();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(INTEGER);
        Collections.sort(values);
        int expectation = values.get(values.size() - 1).intValue;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Integer> aggregation = Aggregators.integerMax("intValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Integer> resultAggregation = Aggregators.integerMax("intValue");
        resultAggregation.combine(aggregation);
        int result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongMax() {
        List<Long> values = sampleLongs();
        Collections.sort(values);
        long expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<Long, Long>, Long> aggregation = Aggregators.longMax();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Long, Long>, Long> resultAggregation = Aggregators.longMax();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(LONG);
        Collections.sort(values);
        long expectation = values.get(values.size() - 1).longValue;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> aggregation = Aggregators.longMax("longValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> resultAggregation = Aggregators.longMax("longValue");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMax() {
        List<String> values = sampleStrings();
        Collections.sort(values);
        String expectation = values.get(values.size() - 1);

        Aggregator<Map.Entry<String, String>, String> aggregation = Aggregators.comparableMax();
        for (String value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<String, String>, String> resultAggregation = Aggregators.comparableMax();
        resultAggregation.combine(aggregation);
        String result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMax_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(STRING);
        Collections.sort(values);
        String expectation = values.get(values.size() - 1).stringValue;

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, String> aggregation = Aggregators.comparableMax("stringValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, String> resultAggregation
                = Aggregators.comparableMax("stringValue");
        resultAggregation.combine(aggregation);
        String result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMax_withNull() {
        List<String> values = sampleStrings();
        Collections.sort(values);
        String expectation = values.get(values.size() - 1);
        values.add(null);

        Aggregator<Map.Entry<String, String>, String> aggregation = Aggregators.comparableMax();
        for (String value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<String, String>, String> resultAggregation = Aggregators.comparableMax();
        resultAggregation.combine(aggregation);
        String result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMax_withAttributePath_withNull() {
        List<ValueContainer> values = sampleValueContainers(STRING);
        Collections.sort(values);
        String expectation = values.get(values.size() - 1).stringValue;
        values.add(null);

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, String> aggregation = Aggregators.comparableMax("stringValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, String> resultAggregation
                = Aggregators.comparableMax("stringValue");
        resultAggregation.combine(aggregation);
        String result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = 60 * TimeoutInMillis.MINUTE)
    public void testMaxBy_withAttributePath_withNull() {
        List<ValueContainer> values = sampleValueContainers(STRING);
        Collections.sort(values);
        Map.Entry<ValueContainer, ValueContainer> expectation = createExtractableEntryWithValue(values.get(values.size() - 1), ss);
        values.add(null);

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Map.Entry<ValueContainer, ValueContainer>> aggregation
                = Aggregators.maxBy("stringValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Map.Entry<ValueContainer, ValueContainer>> resultAggregation
                = Aggregators.maxBy("stringValue");
        resultAggregation.combine(aggregation);
        Map.Entry<ValueContainer, ValueContainer> result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

}
