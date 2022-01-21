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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

public final class SumSqlAggregations {

    private SumSqlAggregations() {
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
                return new SumLongSqlAggregation();
            case BIGINT:
            case DECIMAL:
                return new SumDecimalSqlAggregation();
            case REAL:
                return new SumRealSqlAggregation();
            case DOUBLE:
                return new SumDoubleSqlAggregation();
            default:
                throw QueryException.error("Unsupported operand type: " + operandType);
        }
    }

    @NotThreadSafe
    private static final class SumLongSqlAggregation implements SqlAggregation {

        private long sum;
        private boolean initialized;

        @Override
        public void accumulate(Object value) {
            if (value == null) {
                return;
            }

            try {
                sum = Math.addExact(sum, ((Number) value).longValue());
            } catch (ArithmeticException e) {
                throw QueryException.dataException(QueryDataTypeFamily.BIGINT + " overflow in 'SUM' function " +
                        "(consider adding explicit CAST to DECIMAL)");
            }
            initialized = true;
        }

        @Override
        public void combine(SqlAggregation other0) {
            SumLongSqlAggregation other = (SumLongSqlAggregation) other0;

            if (other.initialized) {
                accumulate(other.sum);
            }
        }

        @Override
        public Object collect() {
            return initialized ? sum : null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(sum);
            out.writeBoolean(initialized);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readLong();
            initialized = in.readBoolean();
        }
    }

    @NotThreadSafe
    private static final class SumDecimalSqlAggregation implements SqlAggregation {

        private BigDecimal sum;

        @Override
        public void accumulate(Object value) {
            if (value == null) {
                return;
            }

            if (sum == null) {
                sum = BigDecimal.ZERO;
            }

            BigDecimal decimalValue = value instanceof BigDecimal
                    ? (BigDecimal) value
                    : new BigDecimal((long) value);
            sum = sum.add(decimalValue, DECIMAL_MATH_CONTEXT);
        }

        @Override
        public void combine(SqlAggregation other0) {
            SumDecimalSqlAggregation other = (SumDecimalSqlAggregation) other0;

            accumulate(other.sum);
        }

        @Override
        public Object collect() {
            return sum;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(sum);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readObject();
        }
    }

    @NotThreadSafe
    private static final class SumRealSqlAggregation implements SqlAggregation {

        private float sum;
        private boolean initialized;

        @Override
        public void accumulate(Object value) {
            if (value == null) {
                return;
            }

            sum += (float) value;
            initialized = true;
        }

        @Override
        public void combine(SqlAggregation other0) {
            SumRealSqlAggregation other = (SumRealSqlAggregation) other0;

            if (other.initialized) {
                accumulate(other.sum);
            }
        }

        @Override
        public Object collect() {
            return initialized ? sum : null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeFloat(sum);
            out.writeBoolean(initialized);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readFloat();
            initialized = in.readBoolean();
        }
    }

    @NotThreadSafe
    private static final class SumDoubleSqlAggregation implements SqlAggregation {

        private double sum;
        private boolean initialized;

        @Override
        public void accumulate(Object value) {
            if (value == null) {
                return;
            }

            sum += (double) value;
            initialized = true;
        }

        @Override
        public void combine(SqlAggregation other0) {
            SumDoubleSqlAggregation other = (SumDoubleSqlAggregation) other0;

            if (other.initialized) {
                accumulate(other.sum);
            }
        }

        @Override
        public Object collect() {
            return initialized ? sum : null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeBoolean(initialized);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            sum = in.readDouble();
            initialized = in.readBoolean();
        }
    }
}
