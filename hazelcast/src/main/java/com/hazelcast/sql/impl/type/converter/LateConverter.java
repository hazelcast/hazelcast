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

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Converter with late type resolution.
 */
public final class LateConverter extends Converter {
    /** Singleton instance. */
    public static final LateConverter INSTANCE = new LateConverter();

    private LateConverter() {
        super(ID_LATE, QueryDataTypeFamily.LATE);
    }

    @Override
    public Class<?> getValueClass() {
        return null;
    }

    @Override
    public boolean asBit(Object val) {
        return getConverter(val).asBit(val);
    }

    @Override
    public byte asTinyint(Object val) {
        return getConverter(val).asTinyint(val);
    }

    @Override
    public short asSmallint(Object val) {
        return getConverter(val).asSmallint(val);
    }

    @Override
    public int asInt(Object val) {
        return getConverter(val).asInt(val);
    }

    @Override
    public long asBigint(Object val) {
        return getConverter(val).asBigint(val);
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return getConverter(val).asDecimal(val);
    }

    @Override
    public float asReal(Object val) {
        return getConverter(val).asReal(val);
    }

    @Override
    public double asDouble(Object val) {
        return getConverter(val).asDouble(val);
    }

    @Override
    public String asVarchar(Object val) {
        return getConverter(val).asVarchar(val);
    }

    @Override
    public LocalDate asDate(Object val) {
        return getConverter(val).asDate(val);
    }

    @Override
    public LocalTime asTime(Object val) {
        return getConverter(val).asTime(val);
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        return getConverter(val).asTimestamp(val);
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return getConverter(val).asTimestampWithTimezone(val);
    }

    @Override
    public Object asObject(Object val) {
        return getConverter(val).asObject(val);
    }

    @Override
    public Object convertToSelf(Converter converter, Object val) {
        return converter.convertToSelf(converter, val);
    }

    protected static Converter getConverter(Object val) {
        assert val != null;

        return Converters.getConverter(val.getClass());
    }
}
