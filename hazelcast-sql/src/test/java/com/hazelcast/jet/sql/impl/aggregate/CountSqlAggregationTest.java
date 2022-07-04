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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountSqlAggregationTest {

    @Test
    public void test_default() {
        SqlAggregation aggregation = CountSqlAggregations.from(false, false);

        assertThat(aggregation.collect()).isEqualTo(0L);
    }

    @Test
    public void test_accumulate() {
        SqlAggregation aggregation = CountSqlAggregations.from(false, false);
        aggregation.accumulate(null);
        aggregation.accumulate(1);

        assertThat(aggregation.collect()).isEqualTo(2L);
    }

    @Test
    public void test_accumulateIgnoreNulls() {
        SqlAggregation aggregation = CountSqlAggregations.from(true, false);
        aggregation.accumulate(null);
        aggregation.accumulate(1);

        assertThat(aggregation.collect()).isEqualTo(1L);
    }

    @Test
    public void test_accumulateDistinct() {
        SqlAggregation aggregation = CountSqlAggregations.from(false, true);
        aggregation.accumulate(null);
        aggregation.accumulate(1);
        aggregation.accumulate(1);
        aggregation.accumulate(2);

        assertThat(aggregation.collect()).isEqualTo(3L);
    }

    @Test
    public void test_accumulateIgnoreNullsAndDistinct() {
        SqlAggregation aggregation = CountSqlAggregations.from(true, true);
        aggregation.accumulate(null);
        aggregation.accumulate(1);
        aggregation.accumulate(1);
        aggregation.accumulate(2);

        assertThat(aggregation.collect()).isEqualTo(2L);
    }

    @Test
    public void test_combine() {
        SqlAggregation left = CountSqlAggregations.from(false, false);
        left.accumulate(null);

        SqlAggregation right = CountSqlAggregations.from(false, false);
        right.accumulate(null);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(2L);
        assertThat(right.collect()).isEqualTo(1L);
    }

    @Test
    public void test_serialization() {
        SqlAggregation original = CountSqlAggregations.from(false, false);
        original.accumulate(null);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        SqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
