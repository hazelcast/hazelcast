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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

public final class AvgSqlAggregations {

    private AvgSqlAggregations() {
    }

    public static SqlAggregation from(QueryDataType operandType, boolean distinct) {
        SqlAggregation aggregation = from(operandType);
        return distinct ? new DistinctSqlAggregation(aggregation) : aggregation;
    }

    private static SqlAggregation from(QueryDataType operandType) {
        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return new AvgDecimalSqlAggregation();
            case REAL:
            case DOUBLE:
                return new AvgDoubleSqlAggregation();
            default:
                throw QueryException.error("Unexpected operand type: " + operandType);
        }
    }

    @NotThreadSafe
    private static final class AvgDecimalSqlAggregation implements SqlAggregation {

        private BigDecimal sum;
        private long count;

        @Override
        public void accumulate(Object value) {
            add(value, 1);
        }

        @Override
        public void combine(SqlAggregation other0) {
            AvgDecimalSqlAggregation other = (AvgDecimalSqlAggregation) other0;

            add(other.sum, other.count);
        }

        private void add(Object value, long count) {
            if (value == null) {
                return;
            }

            if (sum == null) {
                sum = BigDecimal.ZERO;
            }

            BigDecimal decimalValue = value instanceof BigDecimal
                    ? (BigDecimal) value
                    : new BigDecimal(((Number) value).longValue());
            sum = sum.add(decimalValue, DECIMAL_MATH_CONTEXT);
            this.count += count;
        }

        @Override
        public Object collect() {
            if (count == 0) {
                return null;
            }

            return sum.divide(QueryDataType.BIGINT.getConverter().asDecimal(count), DECIMAL_MATH_CONTEXT);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(sum);
            out.writeLong(count);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readObject();
            count = in.readLong();
        }
    }

    @NotThreadSafe
    private static final class AvgDoubleSqlAggregation implements SqlAggregation {

        private double sum;
        private long count;

        @Override
        public void accumulate(Object value) {
            add(value, 1);
        }

        @Override
        public void combine(SqlAggregation other0) {
            AvgDoubleSqlAggregation other = (AvgDoubleSqlAggregation) other0;

            add(other.sum, other.count);
        }

        private void add(Object value, long count) {
            if (value == null) {
                return;
            }

            sum += ((Number) value).doubleValue();
            this.count += count;
        }

        @Override
        public Object collect() {
            if (count == 0) {
                return null;
            }

            return sum / QueryDataType.BIGINT.getConverter().asDouble(count);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
        }
    }
}
