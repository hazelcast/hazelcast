/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.SqlTestSupport.TEST_SS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ValueSqlAggregationTest {

    @Test
    public void test_default() {
        ValueSqlAggregation aggregation = new ValueSqlAggregation();

        assertThat(aggregation.collect()).isNull();
    }

    @Test
    public void test_accumulate() {
        ValueSqlAggregation aggregation = new ValueSqlAggregation();
        aggregation.accumulate("v");
        aggregation.accumulate("v");

        assertThat(aggregation.collect()).isEqualTo("v");
    }

    @Test
    public void test_combine() {
        ValueSqlAggregation left = new ValueSqlAggregation();
        left.accumulate(null);

        ValueSqlAggregation right = new ValueSqlAggregation();
        right.accumulate("v");

        left.combine(right);

        assertThat(left.collect()).isEqualTo("v");
        assertThat(right.collect()).isEqualTo("v");
    }

    @Test
    public void test_serialization() {
        ValueSqlAggregation original = new ValueSqlAggregation();
        original.accumulate("v");

        ValueSqlAggregation serialized = TEST_SS.toObject(TEST_SS.toData(original));

        // the value inside ValueSqlAggregate is not deserialized
        assertEquals(TEST_SS.toData("v"), serialized.collect());
    }
}
