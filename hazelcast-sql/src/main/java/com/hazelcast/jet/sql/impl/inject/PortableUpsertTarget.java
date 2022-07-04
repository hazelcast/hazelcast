/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class PortableUpsertTarget implements UpsertTarget {

    private static final Object NOT_SET = new Object();

    private final ClassDefinition classDefinition;

    private final Object[] values;

    PortableUpsertTarget(@Nonnull ClassDefinition classDef) {
        this.classDefinition = classDef;

        this.values = new Object[classDef.getFieldCount()];
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        int fieldIndex = classDefinition.hasField(path) ? classDefinition.getField(path).getIndex() : -1;
        return value -> {
            if (fieldIndex == -1 && value != null) {
                throw QueryException.error("Unable to inject a non-null value to \"" + path + "\"");
            }

            if (fieldIndex > -1) {
                values[fieldIndex] = value;
            }
        };
    }

    @Override
    public void init() {
        Arrays.fill(values, NOT_SET);
    }

    @Override
    public Object conclude() {
        GenericRecord record = toRecord(classDefinition, values);
        Arrays.fill(values, NOT_SET);
        return record;
    }

    private static GenericRecord toRecord(ClassDefinition classDefinition, Object[] values) {
        PortableGenericRecordBuilder portable = new PortableGenericRecordBuilder(classDefinition);
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            String name = fieldDefinition.getName();
            FieldType type = fieldDefinition.getType();

            Object value = values[i];
            try {
                switch (type) {
                    case BOOLEAN:
                        ensureNotNull(value);
                        portable.setBoolean(name, value != NOT_SET && (boolean) value);
                        break;
                    case BYTE:
                        ensureNotNull(value);
                        portable.setInt8(name, value == NOT_SET ? (byte) 0 : (byte) value);
                        break;
                    case SHORT:
                        ensureNotNull(value);
                        portable.setInt16(name, value == NOT_SET ? (short) 0 : (short) value);
                        break;
                    case CHAR:
                        ensureNotNull(value);
                        portable.setChar(name, value == NOT_SET ? (char) 0 : (char) value);
                        break;
                    case INT:
                        ensureNotNull(value);
                        portable.setInt32(name, value == NOT_SET ? 0 : (int) value);
                        break;
                    case LONG:
                        ensureNotNull(value);
                        portable.setInt64(name, value == NOT_SET ? 0L : (long) value);
                        break;
                    case FLOAT:
                        ensureNotNull(value);
                        portable.setFloat32(name, value == NOT_SET ? 0F : (float) value);
                        break;
                    case DOUBLE:
                        ensureNotNull(value);
                        portable.setFloat64(name, value == NOT_SET ? 0D : (double) value);
                        break;
                    case DECIMAL:
                        portable.setDecimal(name, value == NOT_SET ? null : (BigDecimal) value);
                        break;
                    case UTF:
                        portable.setString(name, value == NOT_SET ? null : (String) QueryDataType.VARCHAR.convert(value));
                        break;
                    case TIME:
                        portable.setTime(name, value == NOT_SET ? null : (LocalTime) value);
                        break;
                    case DATE:
                        portable.setDate(name, value == NOT_SET ? null : (LocalDate) value);
                        break;
                    case TIMESTAMP:
                        portable.setTimestamp(name, value == NOT_SET ? null : (LocalDateTime) value);
                        break;
                    case TIMESTAMP_WITH_TIMEZONE:
                        portable.setTimestampWithTimezone(name, value == NOT_SET ? null : (OffsetDateTime) value);
                        break;
                    case PORTABLE:
                        portable.setGenericRecord(name, value == NOT_SET ? null : (GenericRecord) value);
                        break;
                    case BOOLEAN_ARRAY:
                        portable.setArrayOfBoolean(name, value == NOT_SET ? null : (boolean[]) value);
                        break;
                    case BYTE_ARRAY:
                        portable.setArrayOfInt8(name, value == NOT_SET ? null : (byte[]) value);
                        break;
                    case SHORT_ARRAY:
                        portable.setArrayOfInt16(name, value == NOT_SET ? null : (short[]) value);
                        break;
                    case CHAR_ARRAY:
                        portable.setArrayOfChar(name, value == NOT_SET ? null : (char[]) value);
                        break;
                    case INT_ARRAY:
                        portable.setArrayOfInt32(name, value == NOT_SET ? null : (int[]) value);
                        break;
                    case LONG_ARRAY:
                        portable.setArrayOfInt64(name, value == NOT_SET ? null : (long[]) value);
                        break;
                    case FLOAT_ARRAY:
                        portable.setArrayOfFloat32(name, value == NOT_SET ? null : (float[]) value);
                        break;
                    case DOUBLE_ARRAY:
                        portable.setArrayOfFloat64(name, value == NOT_SET ? null : (double[]) value);
                        break;
                    case DECIMAL_ARRAY:
                        portable.setArrayOfDecimal(name, value == NOT_SET ? null : (BigDecimal[]) value);
                        break;
                    case UTF_ARRAY:
                        portable.setArrayOfString(name, value == NOT_SET ? null : (String[]) value);
                        break;
                    case TIME_ARRAY:
                        portable.setArrayOfTime(name, value == NOT_SET ? null : (LocalTime[]) value);
                        break;
                    case DATE_ARRAY:
                        portable.setArrayOfDate(name, value == NOT_SET ? null : (LocalDate[]) value);
                        break;
                    case TIMESTAMP_ARRAY:
                        portable.setArrayOfTimestamp(name, value == NOT_SET ? null : (LocalDateTime[]) value);
                        break;
                    case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                        portable.setArrayOfTimestampWithTimezone(name, value == NOT_SET ? null : (OffsetDateTime[]) value);
                        break;
                    case PORTABLE_ARRAY:
                        portable.setArrayOfGenericRecord(name, value == NOT_SET ? null : (GenericRecord[]) value);
                        break;
                    default:
                        throw QueryException.error("Unsupported type: " + type);
                }
            } catch (Exception e) {
                throw QueryException.error("Cannot set value " +
                        (value == null ? "null" : "of type " + value.getClass().getName())
                        + " to field \"" + name + "\" of type " + type + ": " + e.getMessage(), e);
            }
        }
        return portable.build();
    }

    private static void ensureNotNull(Object value) {
        if (value == null) {
            throw QueryException.error("Cannot set NULL to a primitive field");
        }
    }
}
