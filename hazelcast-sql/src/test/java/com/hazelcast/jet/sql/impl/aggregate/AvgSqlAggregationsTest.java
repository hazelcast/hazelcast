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
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class AvgSqlAggregationsTest {

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                new Object[]{QueryDataType.TINYINT},
                new Object[]{QueryDataType.SMALLINT},
                new Object[]{QueryDataType.INT},
                new Object[]{QueryDataType.BIGINT},
                new Object[]{QueryDataType.DECIMAL},
                new Object[]{QueryDataType.REAL},
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
                new Object[]{QueryDataType.TINYINT, (byte) 1, (byte) 2, new BigDecimal("1.5")},
                new Object[]{QueryDataType.TINYINT, null, (byte) 1, new BigDecimal(1)},
                new Object[]{QueryDataType.TINYINT, (byte) 1, null, new BigDecimal(1)},
                new Object[]{QueryDataType.TINYINT, null, null, null},
                new Object[]{QueryDataType.SMALLINT, (short) 1, (short) 2, new BigDecimal("1.5")},
                new Object[]{QueryDataType.SMALLINT, null, (short) 1, new BigDecimal(1)},
                new Object[]{QueryDataType.SMALLINT, (short) 1, null, new BigDecimal(1)},
                new Object[]{QueryDataType.SMALLINT, null, null, null},
                new Object[]{QueryDataType.INT, 1, 2, new BigDecimal("1.5")},
                new Object[]{QueryDataType.INT, null, 1, new BigDecimal(1)},
                new Object[]{QueryDataType.INT, 1, null, new BigDecimal(1)},
                new Object[]{QueryDataType.INT, null, null, null},
                new Object[]{QueryDataType.BIGINT, 1L, 2L, new BigDecimal("1.5")},
                new Object[]{QueryDataType.BIGINT, null, 1L, new BigDecimal(1)},
                new Object[]{QueryDataType.BIGINT, 1L, null, new BigDecimal(1)},
                new Object[]{QueryDataType.BIGINT, null, null, null},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), new BigDecimal(2), new BigDecimal("1.5")},
                new Object[]{QueryDataType.DECIMAL, null, new BigDecimal(1), new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), null, new BigDecimal(1)},
                new Object[]{QueryDataType.DECIMAL, null, null, null},
                new Object[]{QueryDataType.REAL, 1F, 2F, 1.5D},
                new Object[]{QueryDataType.REAL, null, 1F, 1D},
                new Object[]{QueryDataType.REAL, 1F, null, 1D},
                new Object[]{QueryDataType.REAL, null, null, null},
                new Object[]{QueryDataType.DOUBLE, 1D, 2D, 1.5D},
                new Object[]{QueryDataType.DOUBLE, null, 1D, 1D},
                new Object[]{QueryDataType.DOUBLE, 1D, null, 1D},
                new Object[]{QueryDataType.DOUBLE, null, null, null}
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

        assertThat(aggregation.collect()).isEqualTo(new BigDecimal("0.6666666666666666666666666666666666666666666666666666666666666666666666666667"));
    }

    @SuppressWarnings("unused")
    private Object[] values_distinct() {
        return new Object[]{
                new Object[]{QueryDataType.TINYINT, asList((byte) 1, (byte) 1, (byte) 2), new BigDecimal("1.5")},
                new Object[]{QueryDataType.SMALLINT, asList((short) 1, (short) 1, (short) 2), new BigDecimal("1.5")},
                new Object[]{QueryDataType.INT, asList(1, 1, 2), new BigDecimal("1.5")},
                new Object[]{QueryDataType.BIGINT, asList(1L, 1L, 2L), new BigDecimal("1.5")},
                new Object[]{QueryDataType.DECIMAL, asList(new BigDecimal(1), new BigDecimal(1), new BigDecimal(2)), new BigDecimal("1.5")},
                new Object[]{QueryDataType.REAL, asList(1F, 1F, 2F), 1.5D},
                new Object[]{QueryDataType.DOUBLE, asList(1D, 1D, 2D), 1.5D},
        };
    }

    @Test
    @Parameters(method = "values_distinct")
    public void test_accumulateDistinct(QueryDataType operandType, List<Object> values, Object expected) {
        SqlAggregation aggregation = AvgSqlAggregations.from(operandType, true);
        aggregation.accumulate(null);
        values.forEach(aggregation::accumulate);

        assertThat(aggregation.collect()).isEqualTo(expected);
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
