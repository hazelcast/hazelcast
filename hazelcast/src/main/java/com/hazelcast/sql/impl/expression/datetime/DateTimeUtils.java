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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public final class DateTimeUtils {
    private static final DateTimeFormatter FORMATTER_TIMESTAMP_WITH_TIMEZONE =
            new DateTimeFormatterBuilder()
                    .parseStrict()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .parseLenient()
                    .appendOffsetId()
                    .toFormatter();

    private static final DateTimeFormatter FORMATTER_TIMESTAMP =
            new DateTimeFormatterBuilder()
                    .parseStrict()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter();


    private DateTimeUtils() { }

    public static OffsetDateTime asTimestampWithTimezone(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asTimestampWithTimezone(res);
    }

    public static OffsetDateTime parseAsOffsetDateTime(String string) {
        return OffsetDateTime.parse(string, FORMATTER_TIMESTAMP_WITH_TIMEZONE);
    }

    public static LocalDateTime parseAsLocalDateTime(String string) {
        return LocalDateTime.parse(string, FORMATTER_TIMESTAMP);
    }

    public static LocalDate parseAsLocalDate(String string) {
        return LocalDate.parse(string);
    }

    public static double extractField(OffsetDateTime time, ExtractField field) {
        return field.extract(time);
    }
}
