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

/**
 * Converter for {@link java.lang.Float} type.
 */
public final class FloatConverter extends Converter {

    public static final FloatConverter INSTANCE = new FloatConverter();

    private FloatConverter() {
        super(ID_FLOAT, QueryDataTypeFamily.REAL);
    }

    @Override
    public Class<?> getValueClass() {
        return Float.class;
    }

    @Override
    public byte asTinyint(Object val) {
        return (byte) cast(val);
    }

    @Override
    public short asSmallint(Object val) {
        return (short) cast(val);
    }

    @Override
    public int asInt(Object val) {
        return (int) cast(val);
    }

    @Override
    public long asBigint(Object val) {
        return (long) cast(val);
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return BigDecimal.valueOf(cast(val));
    }

    @Override
    public float asReal(Object val) {
        return cast(val);
    }

    @Override
    public double asDouble(Object val) {
        return cast(val);
    }

    @Override
    public String asVarchar(Object val) {
        return Float.toString(cast(val));
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asReal(val);
    }

    private float cast(Object val) {
        return (float) val;
    }
}
