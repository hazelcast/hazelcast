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

package com.hazelcast.sql.impl.type.accessor;

import com.hazelcast.sql.impl.type.GenericType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Converter for {@link java.lang.String} type.
 */
public class StringConverter extends Converter {
    /** Singleton instance. */
    public static StringConverter INSTANCE = new StringConverter();

    @Override
    public Class getClazz() {
        return String.class;
    }

    @Override
    public GenericType getGenericType() {
        return GenericType.VARCHAR;
    }

    @Override
    public boolean asBit(Object val) {
        return asInt(val) != 0;
    }

    @Override
    public final byte asTinyInt(Object val) {
        try {
            return Byte.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final short asSmallInt(Object val) {
        try {
            return Short.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final int asInt(Object val) {
        try {
            return Integer.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final long asBigInt(Object val) {
        try {
            return Long.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final BigDecimal asDecimal(Object val) {
        try {
            return new BigDecimal(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final float asReal(Object val) {
        try {
            return Float.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final double asDouble(Object val) {
        try {
            return Double.valueOf(cast(val));
        }
        catch (NumberFormatException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public final String asVarchar(Object val) {
        return cast(val);
    }

    @Override
    public LocalDate asDate(Object val) {
        try {
            return LocalDate.parse(cast(val));
        }
        catch (DateTimeParseException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public LocalTime asTime(Object val) {
        try {
            return LocalTime.parse(cast(val));
        }
        catch (DateTimeParseException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        try {
            return LocalDateTime.parse(cast(val));
        }
        catch (DateTimeParseException e) {
            throw cannotConvertImplicit(val);
        }
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        try {
            return OffsetDateTime.parse(cast(val));
        }
        catch (DateTimeParseException e) {
            throw cannotConvertImplicit(val);
        }
    }

    private String cast(Object val) {
        return (String)val;
    }

    private StringConverter() {
        // No-op.
    }
}
