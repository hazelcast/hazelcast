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

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.aggregation.TestSamples.addValues;
import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleLongs;
import static com.hazelcast.aggregation.TestSamples.sampleValueContainers;
import static com.hazelcast.aggregation.ValueContainer.ValueType.BIG_DECIMAL;
import static com.hazelcast.aggregation.ValueContainer.ValueType.BIG_INTEGER;
import static com.hazelcast.aggregation.ValueContainer.ValueType.DOUBLE;
import static com.hazelcast.aggregation.ValueContainer.ValueType.INTEGER;
import static com.hazelcast.aggregation.ValueContainer.ValueType.LONG;
import static com.hazelcast.aggregation.ValueContainer.ValueType.NUMBER;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("ConstantConditions")
public class AvgAggregationTest {

    public static final double ERROR = 1e-8;

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalAvg() {
        List<BigDecimal> values = sampleBigDecimals();
        BigDecimal expectation = Sums.sumBigDecimals(values)
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> aggregation = Aggregators.bigDecimalAvg();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> resultAggregation = Aggregators.bigDecimalAvg();
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_DECIMAL);
        BigDecimal sum = Sums.sumValueContainer(values, BIG_DECIMAL);
        BigDecimal expectation = sum.divide(BigDecimal.valueOf(values.size()));

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> aggregation = Aggregators.bigDecimalAvg("bigDecimal");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> resultAggregation
                = Aggregators.bigDecimalAvg("bigDecimal");
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigDecimalAvg_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigDecimalAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigDecimalAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigDecimalAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerAvg() {
        List<BigInteger> values = sampleBigIntegers();
        BigDecimal expectation = new BigDecimal(Sums.sumBigIntegers(values))
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigDecimal> aggregation = Aggregators.bigIntegerAvg();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigDecimal> resultAggregation = Aggregators.bigIntegerAvg();
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_INTEGER);
        BigInteger sum = Sums.sumValueContainer(values, BIG_INTEGER);
        BigDecimal expectation = new BigDecimal(sum)
                .divide(BigDecimal.valueOf(values.size()));

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> aggregation = Aggregators.bigIntegerAvg("bigInteger");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> resultAggregation
                = Aggregators.bigIntegerAvg("bigInteger");
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigIntegerAvg_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigIntegerAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigIntegerAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigIntegerAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleAvg() {
        List<Double> values = sampleDoubles();
        double expectation = Sums.sumDoubles(values) / (double) values.size();

        Aggregator<Map.Entry<Double, Double>, Double> aggregation = Aggregators.doubleAvg();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Double, Double>, Double> resultAggregation = Aggregators.doubleAvg();
        resultAggregation.combine(aggregation);
        Double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(DOUBLE);
        double expectation = (Double) Sums.sumValueContainer(values, DOUBLE) / (double) values.size();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.doubleAvg("doubleValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.doubleAvg("doubleValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testDoubleAvg_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.doubleAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testDoubleAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.doubleAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerAvg() {
        List<Integer> values = sampleIntegers();
        double expectation = (double) Sums.sumIntegers(values) / (double) values.size();

        Aggregator<Map.Entry<Integer, Integer>, Double> aggregation = Aggregators.integerAvg();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Integer, Integer>, Double> resultAggregation = Aggregators.integerAvg();
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(INTEGER);
        double expectation = (Long) Sums.sumValueContainer(values, INTEGER) / (double) values.size();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.integerAvg("intValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.integerAvg("intValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testIntegerAvg_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.integerAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testIntegerAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.integerAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongAvg() {
        List<Long> values = sampleLongs();
        double expectation = (double) Sums.sumLongs(values) / (double) values.size();

        Aggregator<Map.Entry<Long, Long>, Double> aggregation = Aggregators.longAvg();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.longAvg();
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(LONG);
        double expectation = (Long) Sums.sumValueContainer(values, LONG) / (double) values.size();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.longAvg("longValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.longAvg("longValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testLongAvg_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.longAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testLongAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.longAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testGenericAvg() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleLongs());
        values.addAll(sampleDoubles());
        values.addAll(sampleIntegers());
        double expectation = Sums.sumFloatingPointNumbers(values) / (double) values.size();

        Aggregator<Map.Entry<Number, Number>, Double> aggregation = Aggregators.numberAvg();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.numberAvg();
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testGenericAvg_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(NUMBER);
        addValues(values, DOUBLE);
        double expectation = (Double) Sums.sumValueContainer(values, NUMBER) / (double) values.size();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.numberAvg("numberValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(newExtractableEntryWithValue(value));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.numberAvg("numberValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testGenericAvg_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.numberAvg();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testGenericAvg_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.numberAvg("bigDecimal");
        aggregation.accumulate(newExtractableEntryWithValue(null));
    }

    @Nonnull
    protected Map.Entry<ValueContainer, ValueContainer> newExtractableEntryWithValue(ValueContainer value) {
        return createExtractableEntryWithValue(value, ss);
    }

}
