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
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class MaxSqlAggregationTest {

    @Test
    public void test_default() {
        MaxSqlAggregation aggregation = new MaxSqlAggregation(0, QueryDataType.INT);

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
        MaxSqlAggregation aggregation = new MaxSqlAggregation(0, QueryDataType.INT);
        aggregation.accumulate(new Object[]{value1});
        aggregation.accumulate(new Object[]{value2});

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(Object value1, Object value2, Object expected) {
        MaxSqlAggregation left = new MaxSqlAggregation(0, QueryDataType.INT);
        left.accumulate(new Object[]{value1});

        MaxSqlAggregation right = new MaxSqlAggregation(0, QueryDataType.INT);
        right.accumulate(new Object[]{value2});

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
        assertThat(right.collect()).isEqualTo(value2);
    }

    @Test
    public void test_serialization() {
        MaxSqlAggregation original = new MaxSqlAggregation(0, QueryDataType.INT);
        original.accumulate(new Object[]{1});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        MaxSqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
