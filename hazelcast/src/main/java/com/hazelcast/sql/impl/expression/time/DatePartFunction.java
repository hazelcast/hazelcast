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
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.WeekFields;

/**
 * Family of date part functions.
 */
public class DatePartFunction extends UniCallExpression<Integer> {
    /** Unit. */
    private DatePartUnit unit;

    /** Operand type. */
    private transient DataType operandType;

    public DatePartFunction() {
        // No-op.
    }

    public DatePartFunction(Expression operand, DatePartUnit unit) {
        super(operand);

        this.unit = unit;
    }

    @Override
    public Integer eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        }

        if (operandType == null) {
            DataType type = operand.getType();

            switch (type.getType()) {
                case VARCHAR:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                    break;

                default:
                    throw new HazelcastSqlException(-1, "Type cannot be cast to DATE/TIME: " + type);
            }

            operandType = type;
        }

        return doDatePart(unit, operandValue, operandType);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MagicNumber"})
    private static int doDatePart(DatePartUnit unit, Object operandValue, DataType operandType) {
        OffsetDateTime dateTime;

        if (operandType == DataType.VARCHAR) {
            String stringOperandValue = operandType.getConverter().asVarchar(operandValue);

            TemporalValue parsedOperandValue = TemporalUtils.parseAny(stringOperandValue);

            Converter converter = parsedOperandValue.getType().getConverter();

            dateTime = converter.asTimestampWithTimezone(parsedOperandValue.getValue());
        } else {
            dateTime = operandType.getConverter().asTimestampWithTimezone(operandValue);
        }

        switch (unit) {
            case YEAR:
                return dateTime.getYear();

            case QUARTER:
                int month = dateTime.getMonthValue();

                return month <= 3 ? 1 : month <= 6 ? 2 : month <= 9 ? 3 : 4;

            case MONTH:
                return dateTime.getMonthValue();

            case WEEK:
                return dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());

            case DAYOFYEAR:
                return dateTime.getDayOfYear();

            case DAYOFMONTH:
                return dateTime.getDayOfMonth();

            case DAYOFWEEK:
                return dateTime.getDayOfWeek().getValue();

            case HOUR:
                return dateTime.getHour();

            case MINUTE:
                return dateTime.getMinute();

            case SECOND:
                return dateTime.getSecond();

            default:
                throw new HazelcastSqlException(-1, "Unsupported unit: " + unit);
        }
    }

    @Override
    public int operator() {
        return CallOperator.EXTRACT;
    }

    @Override
    public DataType getType() {
        return DataType.INT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(unit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        unit = in.readObject();
    }
}
