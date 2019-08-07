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
 * Converter for {@link java.lang.Long} type.
 */
public final class LongConverter extends Converter {
    @Override
    public Class getClazz() {
        return Long.class;
    }

    /** Singleton instance. */
    public static LongConverter INSTANCE = new LongConverter();

    @Override
    public GenericType getGenericType() {
        return GenericType.BIGINT;
    }

    @Override
    public boolean asBit(Object val) {
        return cast(val) != 0L;
    }

    @Override
    public final byte asTinyInt(Object val) {
        return (byte) cast(val);
    }

    @Override
    public final short asSmallInt(Object val) {
        return (short) cast(val);
    }

    @Override
    public final int asInt(Object val) {
        return (int) cast(val);
    }

    @Override
    public final long asBigInt(Object val) {
        return cast(val);
    }

    @Override
    public final BigDecimal asDecimal(Object val) {
        return new BigDecimal(cast(val));
    }

    @Override
    public final float asReal(Object val) {
        return cast(val);
    }

    @Override
    public final double asDouble(Object val) {
        return cast(val);
    }

    @Override
    public final String asVarchar(Object val) {
        return Long.toString(cast(val));
    }

    private long cast(Object val) {
        return (long)val;
    }

    private LongConverter() {
        // No-op.
    }
}
