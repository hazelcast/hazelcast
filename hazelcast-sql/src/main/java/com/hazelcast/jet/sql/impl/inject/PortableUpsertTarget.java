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
        Arrays.fill(values, null);
    }

    @Override
    public Object conclude() {
        GenericRecord record = toRecord(classDefinition, values);
        Arrays.fill(values, null);
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
                        portable.setBoolean(name, value != null && (boolean) value);
                        break;
                    case BYTE:
                        portable.setByte(name, value == null ? (byte) 0 : (byte) value);
                        break;
                    case SHORT:
                        portable.setShort(name, value == null ? (short) 0 : (short) value);
                        break;
                    case CHAR:
                        portable.setChar(name, value == null ? (char) 0 : (char) value);
                        break;
                    case INT:
                        portable.setInt(name, value == null ? 0 : (int) value);
                        break;
                    case LONG:
                        portable.setLong(name, value == null ? 0L : (long) value);
                        break;
                    case FLOAT:
                        portable.setFloat(name, value == null ? 0F : (float) value);
                        break;
                    case DOUBLE:
                        portable.setDouble(name, value == null ? 0D : (double) value);
                        break;
                    case DECIMAL:
                        portable.setDecimal(name, (BigDecimal) value);
                        break;
                    case UTF:
                        portable.setString(name, (String) QueryDataType.VARCHAR.convert(value));
                        break;
                    case TIME:
                        portable.setTime(name, (LocalTime) value);
                        break;
                    case DATE:
                        portable.setDate(name, (LocalDate) value);
                        break;
                    case TIMESTAMP:
                        portable.setTimestamp(name, (LocalDateTime) value);
                        break;
                    case TIMESTAMP_WITH_TIMEZONE:
                        portable.setTimestampWithTimezone(name, (OffsetDateTime) value);
                        break;
                    case PORTABLE:
                        portable.setGenericRecord(name, (GenericRecord) value);
                        break;
                    case BOOLEAN_ARRAY:
                        portable.setBooleanArray(name, (boolean[]) value);
                        break;
                    case BYTE_ARRAY:
                        portable.setByteArray(name, (byte[]) value);
                        break;
                    case SHORT_ARRAY:
                        portable.setShortArray(name, (short[]) value);
                        break;
                    case CHAR_ARRAY:
                        portable.setCharArray(name, (char[]) value);
                        break;
                    case INT_ARRAY:
                        portable.setIntArray(name, (int[]) value);
                        break;
                    case LONG_ARRAY:
                        portable.setLongArray(name, (long[]) value);
                        break;
                    case FLOAT_ARRAY:
                        portable.setFloatArray(name, (float[]) value);
                        break;
                    case DOUBLE_ARRAY:
                        portable.setDoubleArray(name, (double[]) value);
                        break;
                    case DECIMAL_ARRAY:
                        portable.setDecimalArray(name, (BigDecimal[]) value);
                        break;
                    case UTF_ARRAY:
                        portable.setStringArray(name, (String[]) value);
                        break;
                    case TIME_ARRAY:
                        portable.setTimeArray(name, (LocalTime[]) value);
                        break;
                    case DATE_ARRAY:
                        portable.setDateArray(name, (LocalDate[]) value);
                        break;
                    case TIMESTAMP_ARRAY:
                        portable.setTimestampArray(name, (LocalDateTime[]) value);
                        break;
                    case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                        portable.setTimestampWithTimezoneArray(name, (OffsetDateTime[]) value);
                        break;
                    case PORTABLE_ARRAY:
                        portable.setGenericRecordArray(name, (GenericRecord[]) value);
                        break;
                    default:
                        throw QueryException.error("Unsupported type: " + type);
                }
            } catch (Exception e) {
                throw QueryException.error("Cannot set value " +
                        (value == null ? "null" : " of type " + value.getClass().getName())
                        + " to field \"" + name + "\" of type " + type + ": " + e.getMessage(), e);
            }
        }
        return portable.build();
    }
}
