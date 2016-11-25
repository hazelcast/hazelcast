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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static com.hazelcast.aggregation.TestSamples.sampleValueContainers;
import static com.hazelcast.aggregation.TestSamples.sampleStrings;
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
@Category({QuickTest.class, ParallelTest.class})
public class MinAggregationTest {

    public static final double ERROR = 1e-8;

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalMin() {
        List<BigDecimal> values = sampleBigDecimals();
        Collections.sort(values);
        BigDecimal expectation = values.get(0);

        Aggregator<BigDecimal, BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalMin();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_DECIMAL);
        Collections.sort(values);
        BigDecimal expectation = values.get(0).bigDecimal;

        Aggregator<BigDecimal, ValueContainer, ValueContainer> aggregation = Aggregators.bigDecimalMin("bigDecimal");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerMin() {
        List<BigInteger> values = sampleBigIntegers();
        Collections.sort(values);
        BigInteger expectation = values.get(0);

        Aggregator<BigInteger, BigInteger, BigInteger> aggregation = Aggregators.bigIntegerMin();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigInteger result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_INTEGER);
        Collections.sort(values);
        BigInteger expectation = values.get(0).bigInteger;

        Aggregator<BigInteger, ValueContainer, ValueContainer> aggregation = Aggregators.bigIntegerMin("bigInteger");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        BigInteger result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleMin() {
        List<Double> values = sampleDoubles();
        Collections.sort(values);
        double expectation = values.get(0);

        Aggregator<Double, Double, Double> aggregation = Aggregators.doubleMin();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        Double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(DOUBLE);
        Collections.sort(values);
        double expectation = values.get(0).doubleValue;

        Aggregator<Double, ValueContainer, ValueContainer> aggregation = Aggregators.doubleMin("doubleValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerMin() {
        List<Integer> values = sampleIntegers();
        Collections.sort(values);
        long expectation = values.get(0);

        Aggregator<Integer, Integer, Integer> aggregation = Aggregators.integerMin();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(INTEGER);
        Collections.sort(values);
        int expectation = values.get(0).intValue;

        Aggregator<Integer, ValueContainer, ValueContainer> aggregation = Aggregators.integerMin("intValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        int result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongMin() {
        List<Long> values = sampleLongs();
        Collections.sort(values);
        long expectation = values.get(0);

        Aggregator<Long, Long, Long> aggregation = Aggregators.longMin();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(LONG);
        Collections.sort(values);
        long expectation = values.get(0).longValue;

        Aggregator<Long, ValueContainer, ValueContainer> aggregation = Aggregators.longMin("longValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMin() {
        List<String> values = sampleStrings();
        Collections.sort(values);
        String expectation = values.get(0);

        Aggregator<String, String, String> aggregation = Aggregators.comparableMin();
        for (String value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        String result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMin_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(STRING);
        Collections.sort(values);
        String expectation = values.get(0).stringValue;

        Aggregator<String, ValueContainer, ValueContainer> aggregation = Aggregators.comparableMin("stringValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        String result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }
}
