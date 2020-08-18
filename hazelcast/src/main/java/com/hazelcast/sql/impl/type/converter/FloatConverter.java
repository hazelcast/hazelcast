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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;

/**
 * Converter for {@link java.lang.Float} type.
 */
public final class FloatConverter extends Converter {

    public static final FloatConverter INSTANCE = new FloatConverter();

    private FloatConverter() {
        super(ID_FLOAT, REAL);
    }

    @Override
    public Class<?> getValueClass() {
        return Float.class;
    }

    @Override
    public byte asTinyint(Object val) {
        float casted = cast(val);
        if (!Float.isFinite(casted)) {
            throw cannotConvert(QueryDataTypeFamily.TINYINT, val);
        }

        // here the overflow may happen: (byte) casted = (byte) (int) casted
        byte converted = (byte) casted;

        // casts from float to int are saturating
        if (converted != (int) casted) {
            throw numericOverflow(QueryDataTypeFamily.TINYINT, val);
        }

        return converted;
    }

    @Override
    public short asSmallint(Object val) {
        float casted = cast(val);
        if (!Float.isFinite(casted)) {
            throw cannotConvert(QueryDataTypeFamily.SMALLINT, val);
        }

        // here the overflow may happen: (short) casted = (short) (int) casted
        short converted = (short) casted;

        // casts from float to int are saturating
        if (converted != (int) casted) {
            throw numericOverflow(QueryDataTypeFamily.SMALLINT, val);
        }

        return converted;
    }

    @Override
    public int asInt(Object val) {
        float casted = cast(val);
        if (!Float.isFinite(casted)) {
            throw cannotConvert(QueryDataTypeFamily.INT, val);
        }

        // casts from float to int are saturating
        int converted = (int) casted;

        // casts from float to long are saturating
        if (converted != (long) casted) {
            throw numericOverflow(QueryDataTypeFamily.INT, val);
        }

        return converted;
    }

    @Override
    public long asBigint(Object val) {
        float casted = cast(val);
        if (!Float.isFinite(casted)) {
            throw cannotConvert(QueryDataTypeFamily.BIGINT, val);
        }

        float truncated = (float) (casted > 0.0 ? Math.floor(casted) : Math.ceil(casted));
        // casts from float to long are saturating
        long converted = (long) truncated;

        if ((float) converted != truncated) {
            throw numericOverflow(QueryDataTypeFamily.BIGINT, val);
        }

        return converted;
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        float val0 = cast(val);

        if (Float.isInfinite(val0)) {
            throw QueryException.dataException("Cannot convert infinite " + REAL + " value to " + DECIMAL);
        }

        if (Float.isNaN(val0)) {
            throw QueryException.dataException("Cannot convert NaN " + REAL + " value to " + DECIMAL);
        }

        return new BigDecimal(val0, DECIMAL_MATH_CONTEXT);
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
