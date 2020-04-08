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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;

public final class DateTimeExpressionUtils {
    /** Default timestamp precision (nanos). */
    public static final int NANO_PRECISION = 9;

    /** Number of nanoseconds in a second. */
    public static final int NANO_IN_SECONDS = 1_000_000_000;

    /** Order multiplication step. */
    private static final int MULTIPLY_STEP = 10;

    private static final int[] PRECISION_DIVISORS = new int[NANO_PRECISION + 1];

    static {
        int divisor = 1;

        for (int i = NANO_PRECISION; i >= 0; i--) {
            PRECISION_DIVISORS[i] = divisor;

            divisor *= MULTIPLY_STEP;
        }
    }

    private DateTimeExpressionUtils() {
        // No-op.
    }

    /**
     * Convert provided string to any supported date/time object.
     *
     * @param input Input.
     * @return Mathing date/time.
     */
    public static TemporalValue parseAny(String input) {
        if (input == null) {
            return null;
        }

        try {
            if (input.contains("T") || input.contains("t")) {
                // Looks like it is a timestamp.
                if (input.contains("+")) {
                    // Time zone present.
                    return new TemporalValue(
                        QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                        OffsetDateTime.parse(input)
                    );
                } else {
                    // No time zone.
                    return new TemporalValue(QueryDataType.TIMESTAMP, LocalDateTime.parse(input));
                }
            } else if (input.contains("-")) {
                // Looks like it is a date.
                return new TemporalValue(QueryDataType.DATE, LocalDate.parse(input));
            } else {
                // Otherwise it is a time.
                return new TemporalValue(QueryDataType.TIME, LocalTime.parse(input));
            }
        } catch (DateTimeParseException ignore) {
            throw QueryException.error("Failed to parse a string to DATE/TIME: " + input);
        }
    }

    public static int getDivisorForPrecision(int precision) {
        assert precision >= 0 && precision <= NANO_PRECISION;

        return PRECISION_DIVISORS[precision];
    }

    public static int getPrecision(Row row, Expression<?> operand, ExpressionEvalContext context) {
        if (operand == null) {
            return DateTimeExpressionUtils.NANO_PRECISION;
        }

        Object operandValue = operand.eval(row, context);

        if (operandValue == null) {
            return DateTimeExpressionUtils.NANO_PRECISION;
        }

        Integer res = Eval.asInt(operand, row, context);

        if (res == null) {
            return DateTimeExpressionUtils.NANO_PRECISION;
        } else {
            return Math.min(res, DateTimeExpressionUtils.NANO_PRECISION);
        }
    }

    public static OffsetDateTime getCurrentTimestamp(int precision) {
        OffsetDateTime res = OffsetDateTime.now();

        if (precision < DateTimeExpressionUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    public static LocalDateTime getLocalTimestamp(int precision) {
        LocalDateTime res = LocalDateTime.now();

        if (precision < DateTimeExpressionUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    public static LocalTime getLocalTime(int precision) {
        LocalTime res = LocalTime.now();

        if (precision < DateTimeExpressionUtils.NANO_PRECISION) {
            res = res.withNano(getNano(res.getNano(), precision));
        }

        return res;
    }

    public static LocalDate getLocalDate() {
        return LocalDate.now();
    }

    /**
     * Get adjusted nanos.
     *
     * @param nano Nano.
     * @param precision Precision.
     * @return Adjusted nanos.
     */
    private static int getNano(int nano, int precision) {
        int divisor = getDivisorForPrecision(precision);

        return (nano / divisor) * divisor;
    }

    public static OffsetDateTime parseDateTime(String val) {
        if (val == null) {
            return null;
        }

        TemporalValue parsedOperandValue = DateTimeExpressionUtils.parseAny(val);

        Converter converter = parsedOperandValue.getType().getConverter();

        return converter.asTimestampWithTimezone(parsedOperandValue.getValue());
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:MagicNumber"})
    public static int getDatePart(OffsetDateTime dateTime, DatePartUnit unit) {
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
                throw QueryException.error("Unsupported unit: " + unit);
        }
    }
}
