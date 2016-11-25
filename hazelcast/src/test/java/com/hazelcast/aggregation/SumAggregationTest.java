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
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleFloats;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SumAggregationTest {

    public static final double ERROR = 1e-8;

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalSum() {
        List<BigDecimal> values = sampleBigDecimals();
        BigDecimal expectation = Sums.sumBigDecimals(values);

        Aggregator<BigDecimal, BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalSum();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerSum() {
        List<BigInteger> values = sampleBigIntegers();
        BigInteger expectation = Sums.sumBigIntegers(values);

        Aggregator<BigInteger, BigInteger, BigInteger> aggregation = Aggregators.bigIntegerSum();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigInteger result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleSum() {
        List<Double> values = sampleDoubles();
        double expectation = Sums.sumDoubles(values);

        Aggregator<Double, Double, Double> aggregation = Aggregators.doubleSum();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        Double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerSum() {
        List<Integer> values = sampleIntegers();
        long expectation = Sums.sumIntegers(values);

        Aggregator<Long, Integer, Integer> aggregation = Aggregators.integerSum();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongSum() {
        List<Long> values = sampleLongs();
        long expectation = Sums.sumLongs(values);

        Aggregator<Long, Long, Long> aggregation = Aggregators.longSum();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFixedPointSum() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleLongs());
        values.addAll(sampleIntegers());
        values.addAll(sampleBigIntegers());
        long expectation = Sums.sumFixedPointNumbers(values);

        Aggregator<Long, Number, Number> aggregation = Aggregators.fixedPointSum();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        long result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFloatingPointSum() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleDoubles());
        values.addAll(sampleFloats());
        values.addAll(sampleBigDecimals());
        double expectation = Sums.sumFloatingPointNumbers(values);

        Aggregator<Double, Number, Number> aggregation = Aggregators.floatingPointSum();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }
}
