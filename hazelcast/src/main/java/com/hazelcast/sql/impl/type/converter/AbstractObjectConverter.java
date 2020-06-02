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
 * Base converter for OBJECT and LATE types.
 */
public abstract class AbstractObjectConverter extends Converter {
    protected AbstractObjectConverter(int id, QueryDataTypeFamily typeFamily) {
        super(id, typeFamily);
    }

    @Override
    public boolean asBoolean(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.BOOLEAN).asBoolean(val);
    }

    @Override
    public byte asTinyint(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.TINYINT).asTinyint(val);
    }

    @Override
    public short asSmallint(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.SMALLINT).asSmallint(val);
    }

    @Override
    public int asInt(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.INT).asInt(val);
    }

    @Override
    public long asBigint(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.BIGINT).asBigint(val);
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.DECIMAL).asDecimal(val);
    }

    @Override
    public float asReal(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.REAL).asReal(val);
    }

    @Override
    public double asDouble(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.DOUBLE).asDouble(val);
    }

    @Override
    public LocalDate asDate(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.DATE).asDate(val);
    }

    @Override
    public LocalTime asTime(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.TIME).asTime(val);
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.TIMESTAMP).asTimestamp(val);
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return resolveConverter(val, QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE).asTimestampWithTimezone(val);
    }

    protected Converter resolveConverter(Object val, QueryDataTypeFamily target) {
        Converter converter = Converters.getConverter(val.getClass());

        if (converter == this) {
            throw cannotConvert(target);
        }

        return converter;
    }
}
