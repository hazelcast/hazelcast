/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class AvgSqlAggregationTest {

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                new Object[]{QueryDataType.DECIMAL},
                new Object[]{QueryDataType.DOUBLE}
        };
    }

    @Test
    @Parameters(method = "types")
    public void test_default(QueryDataType operandType) {
        SqlAggregation aggregation = AvgSqlAggregations.from(operandType, false);

        assertThat(aggregation.collect()).isNull();
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), new BigDecimal(2), new BigDecimal("1.5")},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal("9223372036854775808998"),
                        new BigDecimal("9223372036854775808999"), new BigDecimal("9223372036854775808998.5")},
                new Object[]{QueryDataType.DOUBLE, 1D, 2D, 1.5D},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), null, new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, null, new BigDecimal(1), new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, null, null, null},
                new Object[]{QueryDataType.DOUBLE, 1D, null, 1D},
                new Object[]{QueryDataType.DOUBLE, null, 1D, 1D},
                new Object[]{QueryDataType.DOUBLE, null, null, null},
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_accumulate(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SqlAggregation aggregation = AvgSqlAggregations.from(operandType, false);
        aggregation.accumulate(value1);
        aggregation.accumulate(value2);

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    public void test_periodicDecimal() {
        SqlAggregation aggregation = AvgSqlAggregations.from(QueryDataType.DECIMAL, false);
        aggregation.accumulate(BigDecimal.ZERO);
        aggregation.accumulate(BigDecimal.ONE);
        aggregation.accumulate(BigDecimal.ONE);

        assertThat(aggregation.collect()).isEqualTo(new BigDecimal("0.66666666666666666666666666666666666667"));
    }

    @Test
    public void test_accumulateDistinct() {
        SqlAggregation aggregation = AvgSqlAggregations.from(QueryDataType.DECIMAL, true);
        aggregation.accumulate(null);
        aggregation.accumulate(BigDecimal.ONE);
        aggregation.accumulate(BigDecimal.ONE);
        aggregation.accumulate(new BigDecimal(2L));

        assertThat(aggregation.collect()).isEqualTo(new BigDecimal("1.5"));
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SqlAggregation left = AvgSqlAggregations.from(operandType, false);
        left.accumulate(value1);

        SqlAggregation right = AvgSqlAggregations.from(operandType, false);
        right.accumulate(value2);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
    }

    @Test
    public void test_serialization() {
        SqlAggregation original = AvgSqlAggregations.from(QueryDataType.DECIMAL, false);
        original.accumulate(BigDecimal.ONE);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        SqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
