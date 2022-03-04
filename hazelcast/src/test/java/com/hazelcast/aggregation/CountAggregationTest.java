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
import java.util.List;
import java.util.Map;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.createExtractableEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.samplePersons;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountAggregationTest {

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator() {
        List<BigDecimal> values = sampleBigDecimals();
        long expectation = values.size();

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> aggregation = Aggregators.count();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath() {
        List<Person> values = samplePersons();
        long expectation = values.size();

        Aggregator<Map.Entry<Person, Person>, Long> aggregation = Aggregators.count("age");
        for (Person person : values) {
            aggregation.accumulate(createExtractableEntryWithValue(person, ss));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count("age");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withNull() {
        List<BigDecimal> values = sampleBigDecimals();
        values.add(null);
        long expectation = values.size();

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> aggregation = Aggregators.count();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath_withNull() {
        List<Person> values = samplePersons();
        values.add(null);
        long expectation = values.size();

        Aggregator<Map.Entry<Person, Person>, Long> aggregation = Aggregators.count("age");
        for (Person person : values) {
            aggregation.accumulate(createExtractableEntryWithValue(person, ss));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count("age");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }
}
