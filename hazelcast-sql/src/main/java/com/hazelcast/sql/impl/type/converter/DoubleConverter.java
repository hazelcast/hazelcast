/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.DECIMAL_MATH_CONTEXT;

/**
 * Converter for {@link java.lang.Double} type.
 */
@SerializableByConvention
public final class DoubleConverter extends Converter {

    public static final DoubleConverter INSTANCE = new DoubleConverter();

    private DoubleConverter() {
        super(ID_DOUBLE, QueryDataTypeFamily.DOUBLE);
    }

    @Override
    public Class<?> getValueClass() {
        return Double.class;
    }

    @Override
    public byte asTinyint(Object val) {
        double val0 = cast(val);

        if (Double.isInfinite(val0)) {
            throw infiniteValueError(QueryDataTypeFamily.TINYINT);
        }

        if (Double.isNaN(val0)) {
            throw nanValueError(QueryDataTypeFamily.TINYINT);
        }

        // here the overflow may happen: (byte) casted = (byte) (int) casted
        byte converted = (byte) val0;

        // casts from double to int are saturating
        if (converted != (int) val0) {
            throw numericOverflowError(QueryDataTypeFamily.TINYINT);
        }

        return converted;
    }

    @Override
    public short asSmallint(Object val) {
        double val0 = cast(val);

        if (Double.isInfinite(val0)) {
            throw infiniteValueError(QueryDataTypeFamily.SMALLINT);
        }

        if (Double.isNaN(val0)) {
            throw nanValueError(QueryDataTypeFamily.SMALLINT);
        }

        // here the overflow may happen: (short) casted = (short) (int) casted
        short converted = (short) val0;

        // casts from double to int are saturating
        if (converted != (int) val0) {
            throw numericOverflowError(QueryDataTypeFamily.SMALLINT);
        }

        return converted;
    }

    @Override
    public int asInt(Object val) {
        double val0 = cast(val);

        if (Double.isInfinite(val0)) {
            throw infiniteValueError(QueryDataTypeFamily.INTEGER);
        }

        if (Double.isNaN(val0)) {
            throw nanValueError(QueryDataTypeFamily.INTEGER);
        }

        int converted = (int) val0;

        // casts from double to long are saturating
        if (converted != (long) val0) {
            throw numericOverflowError(QueryDataTypeFamily.INTEGER);
        }

        return converted;
    }

    @Override
    public long asBigint(Object val) {
        double val0 = cast(val);

        if (Double.isInfinite(val0)) {
            throw infiniteValueError(QueryDataTypeFamily.BIGINT);
        }

        if (Double.isNaN(val0)) {
            throw nanValueError(QueryDataTypeFamily.BIGINT);
        }

        double truncated = val0 > 0.0 ? Math.floor(val0) : Math.ceil(val0);
        // casts from double to long are saturating
        long converted = (long) truncated;

        if ((double) converted != truncated) {
            throw numericOverflowError(QueryDataTypeFamily.BIGINT);
        }

        return converted;
    }

    @SuppressWarnings("checkstyle:TrailingComment")
    @Override
    public BigDecimal asDecimal(Object val) {
        double val0 = cast(val);

        if (Double.isInfinite(val0)) {
            throw infiniteValueError(DECIMAL);
        }

        if (Double.isNaN(val0)) {
            throw nanValueError(DECIMAL);
        }

        return new BigDecimal(val0, DECIMAL_MATH_CONTEXT); //NOSONAR
    }

    @Override
    public float asReal(Object val) {
        double doubleVal = cast(val);
        float floatVal = (float) cast(val);

        if (Float.isInfinite(floatVal) && !Double.isInfinite(doubleVal)) {
            throw numericOverflowError(QueryDataTypeFamily.REAL);
        }

        return floatVal;
    }

    @Override
    public double asDouble(Object val) {
        return cast(val);
    }

    @Override
    public String asVarchar(Object val) {
        return Double.toString(cast(val));
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asDouble(val);
    }

    private double cast(Object val) {
        return (double) val;
    }

}
