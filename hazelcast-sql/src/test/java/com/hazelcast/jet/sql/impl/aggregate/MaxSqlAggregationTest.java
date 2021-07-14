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
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class MaxSqlAggregationTest {

    @Test
    public void test_default() {
        MaxSqlAggregation aggregation = new MaxSqlAggregation();

        assertThat(aggregation.collect()).isNull();
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{null, null, null},
                new Object[]{null, 1, 1},
                new Object[]{1, null, 1},
                new Object[]{1, 1, 1},
                new Object[]{1, 2, 2},
                new Object[]{2, 1, 2}
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_accumulate(Object value1, Object value2, Object expected) {
        MaxSqlAggregation aggregation = new MaxSqlAggregation();
        aggregation.accumulate(value1);
        aggregation.accumulate(value2);

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(Object value1, Object value2, Object expected) {
        MaxSqlAggregation left = new MaxSqlAggregation();
        left.accumulate(value1);

        MaxSqlAggregation right = new MaxSqlAggregation();
        right.accumulate(value2);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
        assertThat(right.collect()).isEqualTo(value2);
    }

    @Test
    public void test_serialization() {
        MaxSqlAggregation original = new MaxSqlAggregation();
        original.accumulate(1);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        MaxSqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
