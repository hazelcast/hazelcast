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

/**
 * Converter for {@link java.lang.Boolean} type.
 */
public final class BooleanConverter extends Converter {
    /** Singleton instance. */
    public static final BooleanConverter INSTANCE = new BooleanConverter();

    private BooleanConverter() {
        // No-op.
    }

    @Override
    public Class getClazz() {
        return Boolean.class;
    }

    @Override
    public GenericType getGenericType() {
        return GenericType.BIT;
    }

    @Override
    public boolean asBit(Object val) {
        return cast(val) != 0;
    }

    @Override
    public byte asTinyInt(Object val) {
        return cast(val);
    }

    @Override
    public short asSmallInt(Object val) {
        return cast(val);
    }

    @Override
    public int asInt(Object val) {
        return cast(val);
    }

    @Override
    public long asBigInt(Object val) {
        return cast(val);
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return cast(val) == 0 ? BigDecimal.ZERO : BigDecimal.ONE;
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
        return Byte.toString(cast(val));
    }

    private byte cast(Object val) {
        return (Boolean) val ? (byte) 1 : (byte) 0;
    }
}
