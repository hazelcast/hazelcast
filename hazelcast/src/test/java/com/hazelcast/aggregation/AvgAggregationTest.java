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

import static com.hazelcast.aggregation.NumberContainer.ValueType.BIG_DECIMAL;
import static com.hazelcast.aggregation.NumberContainer.ValueType.BIG_INTEGER;
import static com.hazelcast.aggregation.NumberContainer.ValueType.DOUBLE;
import static com.hazelcast.aggregation.NumberContainer.ValueType.INTEGER;
import static com.hazelcast.aggregation.NumberContainer.ValueType.LONG;
import static com.hazelcast.aggregation.NumberContainer.ValueType.NUMBER;
import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static com.hazelcast.aggregation.TestSamples.sampleNumberContainers;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("ConstantConditions")
public class AvgAggregationTest {

    public static final double ERROR = 1e-8;

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalAvg() {
        List<BigDecimal> values = sampleBigDecimals();
        BigDecimal expectation = Sums.sumBigDecimals(values)
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<BigDecimal, BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalAvg();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(BIG_DECIMAL);
        BigDecimal sum = Sums.sumNumberContainer(values, BIG_DECIMAL);
        BigDecimal expectation = sum.divide(BigDecimal.valueOf(values.size()));

        Aggregator<BigDecimal, NumberContainer, NumberContainer> aggregation = Aggregators.bigDecimalAvg("bigDecimal");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerAvg() {
        List<BigInteger> values = sampleBigIntegers();
        BigDecimal expectation = new BigDecimal(Sums.sumBigIntegers(values))
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<BigDecimal, BigInteger, BigInteger> aggregation = Aggregators.bigIntegerAvg();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(BIG_INTEGER);
        BigInteger sum = Sums.sumNumberContainer(values, BIG_INTEGER);
        BigDecimal expectation = new BigDecimal(sum)
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<BigDecimal, NumberContainer, NumberContainer> aggregation = Aggregators.bigIntegerAvg("bigInteger");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        BigDecimal result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleAvg() {
        List<Double> values = sampleDoubles();
        double expectation = Sums.sumDoubles(values) / (double) values.size();

        Aggregator<Double, Double, Double> aggregation = Aggregators.doubleAvg();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        Double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(DOUBLE);
        double expectation = (Double) Sums.sumNumberContainer(values, DOUBLE) / (double) values.size();

        Aggregator<Double, NumberContainer, NumberContainer> aggregation = Aggregators.doubleAvg("doubleValue");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerAvg() {
        List<Integer> values = sampleIntegers();
        double expectation = (double) Sums.sumIntegers(values) / (double) values.size();

        Aggregator<Double, Integer, Integer> aggregation = Aggregators.integerAvg();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(INTEGER);
        double expectation = (Long) Sums.sumNumberContainer(values, INTEGER) / (double) values.size();

        Aggregator<Double, NumberContainer, NumberContainer> aggregation = Aggregators.integerAvg("intValue");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongAvg() {
        List<Long> values = sampleLongs();
        double expectation = (double) Sums.sumLongs(values) / (double) values.size();

        Aggregator<Double, Long, Long> aggregation = Aggregators.longAvg();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(LONG);
        double expectation = (Long) Sums.sumNumberContainer(values, LONG) / (double) values.size();

        Aggregator<Double, NumberContainer, NumberContainer> aggregation = Aggregators.longAvg("longValue");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testGenericAvg() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleLongs());
        values.addAll(sampleDoubles());
        values.addAll(sampleIntegers());
        double expectation = Sums.sumFloatingPointNumbers(values) / (double) values.size();

        Aggregator<Double, Number, Number> aggregation = Aggregators.numberAvg();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testGenericAvg_withAttributePath() {
        List<NumberContainer> values = sampleNumberContainers(NUMBER);
        double expectation = (Double) Sums.sumNumberContainer(values, NUMBER) / (double) values.size();

        Aggregator<Double, NumberContainer, NumberContainer> aggregation = Aggregators.numberAvg("numberValue");
        for (NumberContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value));
        }
        double result = aggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }
}
