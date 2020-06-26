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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

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

        throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "VARCHAR value cannot be converted to BOOLEAN: " + val);
    }

    @Override
    public final byte asTinyint(Object val) {
        try {
            return Byte.parseByte(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.TINYINT, val);
        }
    }

    @Override
    public final short asSmallint(Object val) {
        try {
            return Short.parseShort(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.SMALLINT, val);
        }
    }

    @Override
    public final int asInt(Object val) {
        try {
            return Integer.parseInt(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.INT, val);
        }
    }

    @Override
    public final long asBigint(Object val) {
        try {
            return Long.parseLong(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.BIGINT, val);
        }
    }

    @Override
    public final BigDecimal asDecimal(Object val) {
        try {
            return new BigDecimal(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.DECIMAL, val);
        }
    }

    @Override
    public final float asReal(Object val) {
        try {
            return Float.parseFloat(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.REAL, val);
        }
    }

    @Override
    public final double asDouble(Object val) {
        try {
            return Double.parseDouble(cast(val));
        } catch (NumberFormatException e) {
            throw cannotConvert(QueryDataTypeFamily.DOUBLE, val);
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
            throw cannotConvert(QueryDataTypeFamily.DATE, val);
        }
    }

    @Override
    public final LocalTime asTime(Object val) {
        try {
            return LocalTime.parse(cast(val));
        } catch (DateTimeParseException e) {
            throw cannotConvert(QueryDataTypeFamily.TIME, val);
        }
    }

    @Override
    public final LocalDateTime asTimestamp(Object val) {
        try {
            return LocalDateTime.parse(cast(val));
        } catch (DateTimeParseException e) {
            throw cannotConvert(QueryDataTypeFamily.TIMESTAMP, val);
        }
    }

    @Override
    public final OffsetDateTime asTimestampWithTimezone(Object val) {
        try {
            return OffsetDateTime.parse(cast(val));
        } catch (DateTimeParseException e) {
            throw cannotConvert(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE, val);
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
}
