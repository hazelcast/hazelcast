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
import java.util.List;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;
import static com.hazelcast.aggregation.TestSamples.samplePersons;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CountAggregationTest {

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator() {
        List<BigDecimal> values = sampleBigDecimals();
        long expectation = values.size();

        Aggregator<Long, BigDecimal, BigDecimal> aggregation = Aggregators.count();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Long, BigDecimal, BigDecimal> resultAggregation = Aggregators.count();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath() {
        List<Person> values = samplePersons();
        long expectation = values.size();

        Aggregator<Long, Person, Person> aggregation = Aggregators.count("age");
        for (Person person : values) {
            aggregation.accumulate(createEntryWithValue(person));
        }

        Aggregator<Long, BigDecimal, BigDecimal> resultAggregation = Aggregators.count();Aggregators.count("age");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }
}
