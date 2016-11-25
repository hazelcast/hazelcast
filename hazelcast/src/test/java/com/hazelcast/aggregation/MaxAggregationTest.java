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
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static com.hazelcast.aggregation.TestSamples.sampleStrings;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MaxAggregationTest {

    public static final double ERROR = 1e-8;

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalMax() {
        List<BigDecimal> values = sampleBigDecimals();
        Collections.sort(values);

        BigDecimal expectation = values.get(values.size() - 1);

        Aggregator<BigDecimal, BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalMax();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerMax() {
        List<BigInteger> values = sampleBigIntegers();
        Collections.sort(values);

        BigInteger expectation = values.get(values.size() - 1);

        Aggregator<BigInteger, BigInteger, BigInteger> aggregation = Aggregators.bigIntegerMax();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigInteger result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleMax() {
        List<Double> values = sampleDoubles();
        Collections.sort(values);

        double expectation = values.get(values.size() - 1);

        Aggregator<Double, Double, Double> aggregation = Aggregators.doubleMax();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        Double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerMax() {
        List<Integer> values = sampleIntegers();
        Collections.sort(values);

        long expectation = values.get(values.size() - 1);

        Aggregator<Integer, Integer, Integer> aggregation = Aggregators.integerMax();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongMax() {
        List<Long> values = sampleLongs();
        Collections.sort(values);

        long expectation = values.get(values.size() - 1);

        Aggregator<Long, Long, Long> aggregation = Aggregators.longMax();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testComparableMax() {
        List<String> values = sampleStrings();
        Collections.sort(values);

        String expectation = values.get(values.size() - 1);

        Aggregator<String, String, String> aggregation = Aggregators.comparableMax();
        for (String value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        String result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }
}
