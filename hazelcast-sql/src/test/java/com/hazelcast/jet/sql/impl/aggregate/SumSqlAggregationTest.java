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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class SumSqlAggregationTest {

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                new Object[]{QueryDataType.BIGINT},
                new Object[]{QueryDataType.DECIMAL},
                new Object[]{QueryDataType.DOUBLE}
        };
    }

    @Test
    @Parameters(method = "types")
    public void test_default(QueryDataType operandType) {
        SqlAggregation aggregation = SumSqlAggregations.from(operandType, false);

        assertThat(aggregation.collect()).isNull();
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{QueryDataType.BIGINT, 1L, 2L, 3L},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), new BigDecimal(2),
                        new BigDecimal(3)},
                new Object[]{QueryDataType.REAL, 1F, 2F, 3F},
                new Object[]{QueryDataType.DOUBLE, 1D, 2D, 3D},
                new Object[]{QueryDataType.BIGINT, 1L, null, 1L},
                new Object[]{QueryDataType.BIGINT, null, 1L, 1L},
                new Object[]{QueryDataType.BIGINT, null, null, null},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), null, new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, null, new BigDecimal(1), new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, null, null, null},
                new Object[]{QueryDataType.REAL, 1F, null, 1F},
                new Object[]{QueryDataType.REAL, null, 1F, 1F},
                new Object[]{QueryDataType.REAL, null, null, null},
                new Object[]{QueryDataType.DOUBLE, 1D, null, 1D},
                new Object[]{QueryDataType.DOUBLE, null, 1D, 1D},
                new Object[]{QueryDataType.DOUBLE, null, null, null},
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_accumulate(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SqlAggregation aggregation = SumSqlAggregations.from(operandType, false);
        aggregation.accumulate(value1);
        aggregation.accumulate(value2);

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    public void test_accumulateOverflow() {
        SqlAggregation aggregation = SumSqlAggregations.from(QueryDataType.BIGINT, false);
        aggregation.accumulate(Long.MAX_VALUE);

        assertThatThrownBy(() -> aggregation.accumulate(1L))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("BIGINT overflow");
    }

    @Test
    public void test_accumulateDistinct() {
        SqlAggregation aggregation = SumSqlAggregations.from(QueryDataType.BIGINT, true);
        aggregation.accumulate(null);
        aggregation.accumulate(1L);
        aggregation.accumulate(1L);
        aggregation.accumulate(2L);

        assertThat(aggregation.collect()).isEqualTo(3L);
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SqlAggregation left = SumSqlAggregations.from(operandType, false);
        left.accumulate(value1);

        SqlAggregation right = SumSqlAggregations.from(operandType, false);
        right.accumulate(value2);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
    }

    @Test
    public void test_serialization() {
        SqlAggregation original = SumSqlAggregations.from(QueryDataType.BIGINT, false);
        original.accumulate(1L);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        SqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
