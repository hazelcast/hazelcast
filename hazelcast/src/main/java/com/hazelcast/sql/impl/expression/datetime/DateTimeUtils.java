/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class DateTimeUtils {

    private DateTimeUtils() {
    }

    public static OffsetDateTime asTimestampWithTimezone(long millis, ZoneId zoneId) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId);
    }

    public static OffsetDateTime asTimestampWithTimezone(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asTimestampWithTimezone(res);
    }

    public static double extractField(Object object, ExtractField field) {
        if (object instanceof OffsetDateTime) {
            return field.extract((OffsetDateTime) object);
        } else if (object instanceof ZonedDateTime) {
            return field.extract(((ZonedDateTime) object).toOffsetDateTime());
        } else if (object instanceof LocalDateTime) {
            return field.extract((LocalDateTime) object);
        } else if (object instanceof LocalDate) {
            return field.extract((LocalDate) object);
        } else if (object instanceof LocalTime) {
            return field.extract((LocalTime) object);
        } else {
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(object.getClass());
            throw new IllegalArgumentException("Cannot extract field from " + type.getTypeFamily().getPublicType());
        }
    }
}
