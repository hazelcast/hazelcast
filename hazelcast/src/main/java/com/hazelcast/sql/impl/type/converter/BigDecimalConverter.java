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

import java.math.BigDecimal;

/**
 * Converter for {@link java.math.BigDecimal} type.
 */
public final class BigDecimalConverter extends AbstractDecimalConverter {

    public static final BigDecimalConverter INSTANCE = new BigDecimalConverter();

    private BigDecimalConverter() {
        super(ID_BIG_DECIMAL);
    }

    @Override
    public Class<?> getValueClass() {
        return BigDecimal.class;
    }

    @Override
    public byte asTinyint(Object val) {
        return cast(val).byteValue();
    }

    @Override
    public short asSmallint(Object val) {
        return cast(val).shortValue();
    }

    @Override
    public int asInt(Object val) {
        return cast(val).intValue();
    }

    @Override
    public long asBigint(Object val) {
        return cast(val).longValue();
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return cast(val);
    }

    @Override
    public float asReal(Object val) {
        return cast(val).floatValue();
    }

    @Override
    public double asDouble(Object val) {
        return cast(val).doubleValue();
    }

    @Override
    public String asVarchar(Object val) {
        return cast(val).toString();
    }

    private BigDecimal cast(Object val) {
        return (BigDecimal) val;
    }
}
