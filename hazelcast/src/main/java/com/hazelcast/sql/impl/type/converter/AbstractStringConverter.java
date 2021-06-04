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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

/**
 * Common converter for string-based classes.
 */
public abstract class AbstractStringConverter extends Converter {
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
            return LocalDate.parse(cast(val));
        } catch (DateTimeParseException e) {
            return parseDateWithPossibleLeadingZeroes(val);
        }
    }

    @Override
    public final LocalTime asTime(Object val) {
        try {
            return LocalTime.parse(cast(val));
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.TIME);
        }
    }

    @Override
    public final LocalDateTime asTimestamp(Object val) {
        try {
            return LocalDateTime.parse(cast(val));
        } catch (DateTimeParseException e) {
            throw cannotParseError(QueryDataTypeFamily.TIMESTAMP);
        }
    }

    @Override
    public final OffsetDateTime asTimestampWithTimezone(Object val) {
        try {
            return OffsetDateTime.parse(cast(val));
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

    /**
     * Parse non-standard dates with possible leading zeroes.
     * Before method tried to parse such a date, potential date candidate
     */
    private LocalDate parseDateWithPossibleLeadingZeroes(Object val) {
        if (!(val instanceof String)) {
            throw new DateTimeParseException("Can't parse date from non-String source", "", 0);
        }

        // This pattern checks both cases with and without leading zeroes in date.
        Pattern pattern = Pattern.compile("\\d{4}-([1-9]|[0-1][0-2])-([1-9]|[0-2][0-9]|3[01])$");
        String possibleDate = (String) val;

        if (!pattern.matcher(possibleDate).matches()) {
            throw cannotParseError(QueryDataTypeFamily.DATE);
        }

        final String[] dateParts = possibleDate.split("-");
        if (dateParts.length != 3) {
            throw new DateTimeParseException("Date can contain only 3 parts : year, month and day", possibleDate, 0);
        }
        String monthChars = dateParts[1];
        String dayChars = dateParts[2];

        int year = Integer.parseInt(dateParts[0]);
        int monthNumerical = Integer.parseInt(monthChars);
        int dayNumerical = Integer.parseInt(dayChars);

        // It will throw an exception if query want to cast e.g., 31st of February.
        MonthDay monthDay = MonthDay.of(monthNumerical, dayNumerical);

        return LocalDate.of(year, monthDay.getMonth(), monthDay.getDayOfMonth());
    }

    private static QueryException cannotParseError(QueryDataTypeFamily target) {
        String message = "Cannot parse " + QueryDataTypeFamily.VARCHAR + " value to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }
}
