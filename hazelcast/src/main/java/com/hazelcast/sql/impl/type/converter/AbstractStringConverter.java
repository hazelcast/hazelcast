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
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

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

        if (val0.equalsIgnoreCase(BooleanConverter.TRUE)) {
            return true;
        } else if (val0.equalsIgnoreCase(BooleanConverter.FALSE)) {
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
            throw cannotParseError(QueryDataTypeFamily.DATE);
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

    private static QueryException cannotParseError(QueryDataTypeFamily target) {
        String message = "Cannot parse " + QueryDataTypeFamily.VARCHAR + " value to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }
}
