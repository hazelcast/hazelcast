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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistinctAggregationTest {

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator() {
        List<String> values = repeatTimes(3, TestDoubles.sampleStrings());
        Set<String> expectation = new HashSet<String>(values);

        Aggregator<Set<String>, String, String> aggregation = Aggregators.distinct();
        for (String value : values) {
            aggregation.accumulate(TestDoubles.createEntryWithValue(value));
        }
        Set<String> result = aggregation.aggregate();

        assertThat(result, is(equalTo(expectation)));
    }

    private List<String> repeatTimes(int times, List<String> values) {
        List<String> repeatedValues = new ArrayList<String>();
        for (int i = 0; i < times; i++) {
            repeatedValues.addAll(values);
        }
        return repeatedValues;
    }
}
