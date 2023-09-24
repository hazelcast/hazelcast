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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.AbstractGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@NotThreadSafe
class PortableUpsertTarget extends UpsertTarget {
    private final PortableContext context;
    private final ClassDefinition classDefinition;

    PortableUpsertTarget(ClassDefinition classDefinition, InternalSerializationService serializationService) {
        super(serializationService);
        context = serializationService != null ? serializationService.getPortableContext() : null;
        this.classDefinition = classDefinition;
    }

    @Override
    protected Converter<GenericRecord> createConverter(Stream<Field> fields) {
        return createConverter(classDefinition, fields);
    }

    private Converter<GenericRecord> createConverter(ClassDefinition classDef, Stream<Field> fields) {
        Injector<GenericRecordBuilder> injector = createRecordInjector(fields,
                field -> createInjector(classDef, field.name(), field.type()));
        return value -> {
            if (value == null || (value instanceof GenericRecord
                    && ((AbstractGenericRecord) value).getClassIdentifier().equals(classDef))) {
                return (GenericRecord) value;
            }
            GenericRecordBuilder record = PortableGenericRecordBuilder.withDefaults(classDef);
            injector.set(record, value);
            return record.build();
        };
    }

    private Injector<GenericRecordBuilder> createInjector(ClassDefinition classDef, String path, QueryDataType type) {
        if (!classDef.hasField(path)) {
            return (record, value) -> {
                if (value != null) {
                    throw QueryException.error("Field \"" + path + "\" doesn't exist in Portable class definition");
                }
            };
        }
        Injector<GenericRecordBuilder> injector = createInjector0(classDef, path, type);
        return (record, value) -> {
            try {
                injector.set(record, value);
            } catch (Exception e) {
                throw QueryException.error("Cannot set value " +
                        (value == null ? "null" : "of type " + value.getClass().getName())
                        + " to field \"" + path + "\" of type " + type + ": " + e.getMessage(), e);
            }
        };
    }

    @SuppressWarnings("ReturnCount")
    private Injector<GenericRecordBuilder> createInjector0(ClassDefinition classDef, String path, QueryDataType type) {
        FieldDefinition classField = classDef.getField(path);
        switch (classField.getType()) {
            case BOOLEAN:
                return (record, value) -> record.setBoolean(path, (boolean) ensureNotNull(value));
            case CHAR:
                return (record, value) -> record.setChar(path, (char) ensureNotNull(value));
            case BYTE:
                return (record, value) -> record.setInt8(path, (byte) ensureNotNull(value));
            case SHORT:
                return (record, value) -> record.setInt16(path, (short) ensureNotNull(value));
            case INT:
                return (record, value) -> record.setInt32(path, (int) ensureNotNull(value));
            case LONG:
                return (record, value) -> record.setInt64(path, (long) ensureNotNull(value));
            case FLOAT:
                return (record, value) -> record.setFloat32(path, (float) ensureNotNull(value));
            case DOUBLE:
                return (record, value) -> record.setFloat64(path, (double) ensureNotNull(value));
            case DECIMAL:
                return (record, value) -> record.setDecimal(path, (BigDecimal) value);
            case UTF:
                return (record, value) -> record.setString(path, (String) VARCHAR.convert(value));
            case TIME:
                return (record, value) -> record.setTime(path, (LocalTime) value);
            case DATE:
                return (record, value) -> record.setDate(path, (LocalDate) value);
            case TIMESTAMP:
                return (record, value) -> record.setTimestamp(path, (LocalDateTime) value);
            case TIMESTAMP_WITH_TIMEZONE:
                return (record, value) -> record.setTimestampWithTimezone(path, (OffsetDateTime) value);
            case PORTABLE:
                ClassDefinition fieldDef = context.lookupClassDefinition(classField.getPortableId());
                Converter<GenericRecord> converter = createConverter(fieldDef, getFields(type));
                return (record, value) -> record.setGenericRecord(path, converter.apply(value));
            case BOOLEAN_ARRAY:
                return (record, value) -> record.setArrayOfBoolean(path, (boolean[]) value);
            case BYTE_ARRAY:
                return (record, value) -> record.setArrayOfInt8(path, (byte[]) value);
            case SHORT_ARRAY:
                return (record, value) -> record.setArrayOfInt16(path, (short[]) value);
            case CHAR_ARRAY:
                return (record, value) -> record.setArrayOfChar(path, (char[]) value);
            case INT_ARRAY:
                return (record, value) -> record.setArrayOfInt32(path, (int[]) value);
            case LONG_ARRAY:
                return (record, value) -> record.setArrayOfInt64(path, (long[]) value);
            case FLOAT_ARRAY:
                return (record, value) -> record.setArrayOfFloat32(path, (float[]) value);
            case DOUBLE_ARRAY:
                return (record, value) -> record.setArrayOfFloat64(path, (double[]) value);
            case DECIMAL_ARRAY:
                return (record, value) -> record.setArrayOfDecimal(path, (BigDecimal[]) value);
            case UTF_ARRAY:
                return (record, value) -> record.setArrayOfString(path, (String[]) value);
            case TIME_ARRAY:
                return (record, value) -> record.setArrayOfTime(path, (LocalTime[]) value);
            case DATE_ARRAY:
                return (record, value) -> record.setArrayOfDate(path, (LocalDate[]) value);
            case TIMESTAMP_ARRAY:
                return (record, value) -> record.setArrayOfTimestamp(path, (LocalDateTime[]) value);
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return (record, value) -> record.setArrayOfTimestampWithTimezone(path, (OffsetDateTime[]) value);
            case PORTABLE_ARRAY:
                return (record, value) -> record.setArrayOfGenericRecord(path, (GenericRecord[]) value);
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    private static Object ensureNotNull(Object value) {
        if (value == null) {
            throw QueryException.error("Cannot set NULL to a primitive field");
        }
        return value;
    }
}
