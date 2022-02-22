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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Common converter for string-based classes.
 */
public abstract class AbstractStringConverter extends Converter {
    private static final int MIN_YEAR_SYMBOLS = 4;
    private static final int MAX_YEAR_SYMBOLS = 10;

    // region date-time formatters
    static final DateTimeFormatter STANDARD_DATE_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, MIN_YEAR_SYMBOLS, MAX_YEAR_SYMBOLS, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter();

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:DeclarationOrder"})
    static final DateTimeFormatter STANDARD_TIME_FORMAT = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    static final DateTimeFormatter STANDARD_DATE_TIME_FORMAT = new DateTimeFormatterBuilder()
            .append(STANDARD_DATE_FORMAT)
            .appendPattern("['T'][' ']")
            .append(STANDARD_TIME_FORMAT)
            .toFormatter();

    static final DateTimeFormatter STANDARD_OFFSET_DATE_TIME_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(STANDARD_DATE_TIME_FORMAT)
            .appendOffsetId()
            .toFormatter();

    //endregion

    protected AbstractStringConverter(int id) {
        super(id, QueryDataTypeFamily.VARCHAR);
    }

    @Override
    public Class<?> getNormalizedValueClass() {
        return String.class;
    }

    @Override
    public final boolean asBoolean(Object val) {
        String val0 = cast(val);

        if (equalsIgnoreCase(val0, BooleanConverter.TRUE)) {
            return true;
        } else if (equalsIgnoreCase(val0, BooleanConverter.FALSE)) {
            return false;
        }

        throw cannotParseError(QueryDataTypeFamily.BOOLEAN);
    }

    @Override
    public final byte asTinyint(Object val) {
        try {
            return Byte.parseByte(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.TINYINT);
        }
    }

    @Override
    public final short asSmallint(Object val) {
        try {
            return Short.parseShort(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.SMALLINT);
        }
    }

    @Override
    public final int asInt(Object val) {
        try {
            return Integer.parseInt(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.INTEGER);
        }
    }

    @Override
    public final long asBigint(Object val) {
        try {
            return Long.parseLong(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.BIGINT);
        }
    }

    @Override
    public final BigDecimal asDecimal(Object val) {
        try {
            return new BigDecimal(cast(val), DECIMAL_MATH_CONTEXT);
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.DECIMAL);
        }
    }

    @Override
    public final float asReal(Object val) {
        try {
            return Float.parseFloat(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.REAL);
        }
    }

    @Override
    public final double asDouble(Object val) {
        try {
            return Double.parseDouble(cast(val));
        } catch (NumberFormatException e) {
            throw cannotParseError(QueryDataTypeFamily.DOUBLE);
        }
    }

    @Override
    public final String asVarchar(Object val) {
        return cast(val);
    }

    @Override
    public final LocalDate asDate(Object val) {
        try {
            return LocalDate.parse(cast(val), STANDARD_DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.DATE);
        }
    }

    @Override
    public final LocalTime asTime(Object val) {
        try {
            return LocalTime.parse(cast(val), STANDARD_TIME_FORMAT);
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.TIME);
        }
    }

    @Override
    public final LocalDateTime asTimestamp(Object val) {
        try {
            return LocalDateTime.parse(cast(val), STANDARD_DATE_TIME_FORMAT);
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.TIMESTAMP);
        }
    }

    @Override
    public final OffsetDateTime asTimestampWithTimezone(Object val) {
        try {
            return OffsetDateTime.parse(cast(val), STANDARD_OFFSET_DATE_TIME_FORMAT);
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
        }
    }

    @Override
    public final Object asObject(Object val) {
        return asVarchar(val);
    }

    @Override
    public final Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asVarchar(val);
    }

    protected abstract String cast(Object val);

    private static QueryException cannotParseError(QueryDataTypeFamily target) {
        String message = "Cannot parse " + QueryDataTypeFamily.VARCHAR + " value to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }
}
