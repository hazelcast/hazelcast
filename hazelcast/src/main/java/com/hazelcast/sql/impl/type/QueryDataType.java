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

package com.hazelcast.sql.impl.type;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BigIntegerConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.ByteConverter;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import com.hazelcast.sql.impl.type.converter.CharacterConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.DateConverter;
import com.hazelcast.sql.impl.type.converter.DoubleConverter;
import com.hazelcast.sql.impl.type.converter.FloatConverter;
import com.hazelcast.sql.impl.type.converter.InstantConverter;
import com.hazelcast.sql.impl.type.converter.IntegerConverter;
import com.hazelcast.sql.impl.type.converter.LateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.ShortConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;

import java.io.IOException;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 */
public class QueryDataType implements IdentifiedDataSerializable {
    public static final int PRECISION_BOOLEAN = 1;
    public static final int PRECISION_TINYINT = 4;
    public static final int PRECISION_SMALLINT = 7;
    public static final int PRECISION_INT = 11;
    public static final int PRECISION_BIGINT = 20;
    public static final int PRECISION_UNLIMITED = -1;

    public static final QueryDataType LATE = new QueryDataType(LateConverter.INSTANCE);

    public static final QueryDataType VARCHAR = new QueryDataType(StringConverter.INSTANCE);
    public static final QueryDataType VARCHAR_CHARACTER = new QueryDataType(CharacterConverter.INSTANCE);

    public static final QueryDataType BOOLEAN = new QueryDataType(BooleanConverter.INSTANCE, PRECISION_BOOLEAN);
    public static final QueryDataType TINYINT = new QueryDataType(ByteConverter.INSTANCE, PRECISION_TINYINT);
    public static final QueryDataType SMALLINT = new QueryDataType(ShortConverter.INSTANCE, PRECISION_SMALLINT);
    public static final QueryDataType INT = new QueryDataType(IntegerConverter.INSTANCE, PRECISION_INT);
    public static final QueryDataType BIGINT = new QueryDataType(LongConverter.INSTANCE, PRECISION_BIGINT);
    public static final QueryDataType DECIMAL = new QueryDataType(BigDecimalConverter.INSTANCE);
    public static final QueryDataType DECIMAL_BIG_INTEGER = new QueryDataType(BigIntegerConverter.INSTANCE);
    public static final QueryDataType REAL = new QueryDataType(FloatConverter.INSTANCE);
    public static final QueryDataType DOUBLE = new QueryDataType(DoubleConverter.INSTANCE);

    public static final QueryDataType TIME = new QueryDataType(LocalTimeConverter.INSTANCE);
    public static final QueryDataType DATE = new QueryDataType(LocalDateConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP = new QueryDataType(LocalDateTimeConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_DATE = new QueryDataType(DateConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_CALENDAR = new QueryDataType(CalendarConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_INSTANT = new QueryDataType(InstantConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME = new QueryDataType(OffsetDateTimeConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_ZONED_DATE_TIME = new QueryDataType(ZonedDateTimeConverter.INSTANCE);

    public static final QueryDataType OBJECT = new QueryDataType(ObjectConverter.INSTANCE);

    private Converter converter;
    private int precision;

    public QueryDataType() {
        // No-op.
    }

    QueryDataType(Converter converter) {
        this(converter, PRECISION_UNLIMITED);
    }

    QueryDataType(Converter converter, int precision) {
        this.converter = converter;
        this.precision = precision;
    }

    public QueryDataTypeFamily getTypeFamily() {
        return converter.getTypeFamily();
    }

    public Converter getConverter() {
        return converter;
    }

    public int getPrecision() {
        return precision;
    }

    public Object convert(Object value) {
        if (value == null) {
            return value;
        }

        Class<?> valueClass = value.getClass();

        if (valueClass == converter.getValueClass()) {
            return value;
        }

        return converter.convertToSelf(Converters.getConverter(valueClass), value);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.QUERY_DATA_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(converter.getId());
        out.writeInt(precision);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        converter = Converters.getConverter(in.readInt());
        precision = in.readInt();
    }

    @Override
    public int hashCode() {
        return 31 * converter.getId() + precision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryDataType type = (QueryDataType) o;

        return converter.getId() == type.converter.getId() && precision == type.precision;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {family=" + getTypeFamily() + ", precision=" + precision + "}";
    }
}
