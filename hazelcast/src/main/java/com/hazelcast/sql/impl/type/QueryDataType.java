/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.type.converter.IntervalConverter;
import com.hazelcast.sql.impl.type.converter.JsonConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.MapConverter;
import com.hazelcast.sql.impl.type.converter.NullConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.ShortConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;

import java.io.IOException;
import java.io.Serializable;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 * <p>
 * Java serialization is needed for Jet.
 */
public class QueryDataType implements IdentifiedDataSerializable, Serializable {

    public static final int MAX_DECIMAL_PRECISION = 76;
    public static final int MAX_DECIMAL_SCALE = 38;

    public static final QueryDataType VARCHAR = new QueryDataType(StringConverter.INSTANCE);
    public static final QueryDataType VARCHAR_CHARACTER = new QueryDataType(CharacterConverter.INSTANCE);

    public static final QueryDataType BOOLEAN = new QueryDataType(BooleanConverter.INSTANCE);

    public static final QueryDataType TINYINT = new QueryDataType(ByteConverter.INSTANCE);
    public static final QueryDataType SMALLINT = new QueryDataType(ShortConverter.INSTANCE);
    public static final QueryDataType INT = new QueryDataType(IntegerConverter.INSTANCE);
    public static final QueryDataType BIGINT = new QueryDataType(LongConverter.INSTANCE);
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

    public static final QueryDataType NULL = new QueryDataType(NullConverter.INSTANCE);

    public static final QueryDataType INTERVAL_YEAR_MONTH = new QueryDataType(IntervalConverter.YEAR_MONTH);
    public static final QueryDataType INTERVAL_DAY_SECOND = new QueryDataType(IntervalConverter.DAY_SECOND);

    public static final QueryDataType MAP = new QueryDataType(MapConverter.INSTANCE);
    public static final QueryDataType JSON = new QueryDataType(JsonConverter.INSTANCE);

    private Converter converter;

    public QueryDataType() {
        // No-op.
    }

    QueryDataType(Converter converter) {
        this.converter = converter;
    }

    public QueryDataTypeFamily getTypeFamily() {
        return converter.getTypeFamily();
    }

    public Converter getConverter() {
        return converter;
    }

    /**
     * Normalize the given value to a value returned by this instance. If the value doesn't match
     * the type expected by the converter, an exception is thrown.
     *
     * @param value Value
     * @return Normalized value
     * @throws QueryDataTypeMismatchException In case of data type mismatch.
     */
    public Object normalize(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> valueClass = value.getClass();

        if (valueClass == converter.getNormalizedValueClass()) {
            // Do nothing if the value is already in the normalized form.
            return value;
        }

        if (!converter.getValueClass().isAssignableFrom(valueClass)) {
            // Expected and actual class don't match. Throw an error.
            throw new QueryDataTypeMismatchException(converter.getValueClass(), valueClass);
        }

        return converter.convertToSelf(converter, value);
    }

    /**
     * Normalize the given value to a value returned by this instance. If the value doesn't match
     * the type expected by the converter, a conversion is performed.
     *
     * @param value Value
     * @return Normalized value
     */
    public Object convert(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> valueClass = value.getClass();

        if (valueClass == converter.getNormalizedValueClass()) {
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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        converter = Converters.getConverter(in.readInt());
    }

    @Override
    public int hashCode() {
        return 31 * converter.getId();
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

        return converter.getId() == type.converter.getId();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {family=" + getTypeFamily() + "}";
    }
}
