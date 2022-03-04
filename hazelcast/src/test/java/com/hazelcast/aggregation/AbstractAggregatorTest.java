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
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractAggregatorTest {

    @Test(timeout = TimeoutInMillis.MINUTE, expected = IllegalArgumentException.class)
    public void testBigDecimalAvg_whenNoAttributePathAndNoMapEntry_thenThrowException() {
        List<BigDecimal> values = sampleBigDecimals();

        Aggregator<BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalAvg();
        aggregation.accumulate(values.get(0));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = IllegalArgumentException.class)
    public void testBigDecimalAvg_whenWithAttributePathAndNoExtractable_thenThrowException() {
        List<BigDecimal> values = sampleBigDecimals();

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> aggregation = Aggregators.bigDecimalAvg("notFound");
        aggregation.accumulate(createEntryWithValue(values.get(0)));
    }
}
