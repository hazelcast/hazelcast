/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.time;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Function to get current timestamp.
 */
public class GetTimestampFunction<T> extends UniCallExpression<T> {
    /** Operator. */
    private int operator;

    /** Optional precision. */
    private transient DataType precisionType;

    public GetTimestampFunction() {
        // No-op.
    }

    public GetTimestampFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        int precision = getPrecision(ctx, row);

        switch (operator) {
            case CallOperator.CURRENT_TIMESTAMP:
                return (T) getCurrentTimestamp(precision);

            case CallOperator.LOCAL_TIMESTAMP:
                return (T) getLocalTimestamp(precision);

            case CallOperator.LOCAL_TIME:
                return (T) getLocalTime(precision);

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    private OffsetDateTime getCurrentTimestamp(int precision) {
        OffsetDateTime res = OffsetDateTime.now();

        if (precision < TemporalUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    private LocalDateTime getLocalTimestamp(int precision) {
        LocalDateTime res = LocalDateTime.now();

        if (precision < TemporalUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    private LocalTime getLocalTime(int precision) {
        LocalTime res = LocalTime.now();

        if (precision < TemporalUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    private int getPrecision(QueryContext ctx, Row row) {
        if (operand == null) {
            return TemporalUtils.NANO_PRECISION;
        }

        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return TemporalUtils.NANO_PRECISION;
        }

        if (precisionType == null) {
            DataType type = operand.getType();

            if (!type.isCanConvertToNumeric()) {
                throw new HazelcastSqlException(-1, "Operand type cannot be converted to INT: " + type);
            }

            precisionType = type;
        }

        int res = precisionType.getConverter().asInt(operandValue);

        if (res <= 0) {
            return res;
        } else {
            return Math.min(res, TemporalUtils.NANO_PRECISION);
        }
    }

    /**
     * Get adjusted nanos.
     *
     * @param nano Nano.
     * @param precision Precision.
     * @return Adjusted nanos.
     */
    private static int getNano(int nano, int precision) {
        int divisor = TemporalUtils.getDivisorForPrecision(precision);

        return (nano / divisor) * divisor;
    }

    @Override
    public DataType getType() {
        switch (operator) {
            case CallOperator.CURRENT_TIMESTAMP:
                return DataType.TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME;

            case CallOperator.LOCAL_TIMESTAMP:
                return DataType.TIMESTAMP;

            case CallOperator.LOCAL_TIME:
                return DataType.TIME;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    @Override
    public int operator() {
        return operator;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(operator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operator = in.readInt();
    }
}
