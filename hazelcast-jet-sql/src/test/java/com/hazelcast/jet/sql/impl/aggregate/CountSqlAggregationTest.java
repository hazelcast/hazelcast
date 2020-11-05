/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CountSqlAggregationTest {

    @Test
    public void test_default() {
        CountSqlAggregation aggregation = new CountSqlAggregation();

        assertThat(aggregation.collect()).isEqualTo(0L);
    }

    @Test
    public void test_accumulate() {
        CountSqlAggregation aggregation = new CountSqlAggregation();
        aggregation.accumulate(new Object[]{null});
        aggregation.accumulate(new Object[]{1});

        assertThat(aggregation.collect()).isEqualTo(2L);
    }

    @Test
    public void test_accumulateIgnoreNulls() {
        CountSqlAggregation aggregation = new CountSqlAggregation(0);
        aggregation.accumulate(new Object[]{null});
        aggregation.accumulate(new Object[]{1});

        assertThat(aggregation.collect()).isEqualTo(1L);
    }

    @Test
    public void test_accumulateDistinct() {
        CountSqlAggregation aggregation = new CountSqlAggregation(0, true);
        aggregation.accumulate(new Object[]{null});
        aggregation.accumulate(new Object[]{1});
        aggregation.accumulate(new Object[]{1});
        aggregation.accumulate(new Object[]{2});

        assertThat(aggregation.collect()).isEqualTo(2L);
    }

    @Test
    public void test_combine() {
        CountSqlAggregation left = new CountSqlAggregation();
        left.accumulate(new Object[0]);

        CountSqlAggregation right = new CountSqlAggregation();
        right.accumulate(new Object[0]);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(2L);
        assertThat(right.collect()).isEqualTo(1L);
    }

    @Test
    public void test_serialization() {
        CountSqlAggregation original = new CountSqlAggregation();
        original.accumulate(new Object[0]);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        CountSqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
