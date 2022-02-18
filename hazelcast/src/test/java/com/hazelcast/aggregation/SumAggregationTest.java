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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.aggregation.TestSamples.addValues;
import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.sampleBigIntegers;
import static com.hazelcast.aggregation.TestSamples.sampleDoubles;
import static com.hazelcast.aggregation.TestSamples.sampleFloats;
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
public class SumAggregationTest {

    public static final double ERROR = 1e-8;

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalSum() {
        List<BigDecimal> values = sampleBigDecimals();
        BigDecimal expectation = Sums.sumBigDecimals(values);

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> aggregation = Aggregators.bigDecimalSum();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> resultAggregation = Aggregators.bigDecimalSum();
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigDecimalSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_DECIMAL);
        BigDecimal expectation = Sums.sumValueContainer(values, BIG_DECIMAL);

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> aggregation = Aggregators.bigDecimalSum("bigDecimal");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigDecimal> resultAggregation
                = Aggregators.bigDecimalSum("bigDecimal");
        resultAggregation.combine(aggregation);
        BigDecimal result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigDecimalSum_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigDecimalSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigDecimalSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, BigDecimal> aggregation = Aggregators.bigDecimalSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerSum() {
        List<BigInteger> values = sampleBigIntegers();
        BigInteger expectation = Sums.sumBigIntegers(values);

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigInteger> aggregation = Aggregators.bigIntegerSum();
        for (BigInteger value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigInteger, BigInteger>, BigInteger> resultAggregation = Aggregators.bigIntegerSum();
        resultAggregation.combine(aggregation);
        BigInteger result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testBigIntegerSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(BIG_INTEGER);
        BigInteger expectation = Sums.sumValueContainer(values, BIG_INTEGER);

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigInteger> aggregation = Aggregators.bigIntegerSum("bigInteger");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, BigInteger> resultAggregation
                = Aggregators.bigIntegerSum("bigInteger");
        resultAggregation.combine(aggregation);
        BigInteger result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigIntegerSum_withNull() {
        Aggregator<Map.Entry, BigInteger> aggregation = Aggregators.bigIntegerSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testBigIntegerSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, BigInteger> aggregation = Aggregators.bigIntegerSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleSum() {
        List<Double> values = sampleDoubles();
        double expectation = Sums.sumDoubles(values);

        Aggregator<Map.Entry<Double, Double>, Double> aggregation = Aggregators.doubleSum();
        for (Double value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Double, Double>, Double> resultAggregation = Aggregators.doubleSum();
        resultAggregation.combine(aggregation);
        Double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testDoubleSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(DOUBLE);
        double expectation = Sums.sumValueContainer(values, DOUBLE).doubleValue();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.doubleSum("doubleValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation = Aggregators.doubleSum("doubleValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testDoubleSum_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.doubleSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testDoubleSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.doubleSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerSum() {
        List<Integer> values = sampleIntegers();
        long expectation = Sums.sumIntegers(values);

        Aggregator<Map.Entry<Integer, Integer>, Long> aggregation = Aggregators.integerSum();
        for (Integer value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Integer, Integer>, Long> resultAggregation = Aggregators.integerSum();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testIntegerSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(INTEGER);
        long expectation = Sums.sumValueContainer(values, INTEGER).intValue();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> aggregation = Aggregators.integerSum("intValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> resultAggregation = Aggregators.integerSum("intValue");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testIntegerSum_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.integerSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testIntegerSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.integerSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongSum() {
        List<Long> values = sampleLongs();
        long expectation = Sums.sumLongs(values);

        Aggregator<Map.Entry<Long, Long>, Long> aggregation = Aggregators.longSum();
        for (Long value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Long, Long>, Long> resultAggregation = Aggregators.longSum();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testLongSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(LONG);
        long expectation = Sums.sumValueContainer(values, LONG).longValue();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> aggregation = Aggregators.longSum("longValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> resultAggregation = Aggregators.longSum("longValue");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testLongSum_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.longSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testLongSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.longSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFixedPointSum() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleLongs());
        values.addAll(sampleIntegers());
        values.addAll(sampleBigIntegers());
        long expectation = Sums.sumFixedPointNumbers(values);

        Aggregator<Map.Entry<Number, Number>, Long> aggregation = Aggregators.fixedPointSum();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Number, Number>, Long> resultAggregation = Aggregators.fixedPointSum();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFixedPointSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(NUMBER);
        addValues(values, BIG_INTEGER);
        double expectation = Sums.sumValueContainer(values, NUMBER).doubleValue();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> aggregation = Aggregators.fixedPointSum("numberValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Long> resultAggregation = Aggregators.fixedPointSum("numberValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testFixedPointSum_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.fixedPointSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testFixedPointSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, Long> aggregation = Aggregators.fixedPointSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFloatingPointSum() {
        List<Number> values = new ArrayList<Number>();
        values.addAll(sampleDoubles());
        values.addAll(sampleFloats());
        values.addAll(sampleBigDecimals());
        double expectation = Sums.sumFloatingPointNumbers(values);

        Aggregator<Map.Entry<Number, Number>, Double> aggregation = Aggregators.floatingPointSum();
        for (Number value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<Number, Number>, Double> resultAggregation = Aggregators.floatingPointSum();
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testFloatingPointSum_withAttributePath() {
        List<ValueContainer> values = sampleValueContainers(NUMBER);
        addValues(values, DOUBLE);
        double expectation = Sums.sumValueContainer(values, NUMBER).doubleValue();

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> aggregation = Aggregators.floatingPointSum("numberValue");
        for (ValueContainer value : values) {
            aggregation.accumulate(createExtractableEntryWithValue(value, ss));
        }

        Aggregator<Map.Entry<ValueContainer, ValueContainer>, Double> resultAggregation
                = Aggregators.floatingPointSum("numberValue");
        resultAggregation.combine(aggregation);
        double result = resultAggregation.aggregate();

        assertThat(result, is(closeTo(expectation, ERROR)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testFloatingPointSum_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.floatingPointSum();
        aggregation.accumulate(createEntryWithValue(null));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = NullPointerException.class)
    public void testFloatingPointSum_withAttributePath_withNull() {
        Aggregator<Map.Entry, Double> aggregation = Aggregators.floatingPointSum("numberValue");
        aggregation.accumulate(createExtractableEntryWithValue(null, ss));
    }

}
