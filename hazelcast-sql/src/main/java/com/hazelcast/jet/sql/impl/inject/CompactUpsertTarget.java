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
import com.hazelcast.internal.serialization.impl.compact.DeserializedSchemaBoundGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
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
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;

@NotThreadSafe
class CompactUpsertTarget extends UpsertTarget {
    private final String typeName;
    private final Map<String, Schema> schemas;

    CompactUpsertTarget(String typeName, Map<String, Schema> schemas, InternalSerializationService serializationService) {
        super(serializationService);
        this.typeName = typeName;
        this.schemas = schemas;
    }

    @Override
    protected Converter<GenericRecord> createConverter(Stream<Field> fields) {
        return createConverter(schemas.get(typeName), fields);
    }

    private Converter<GenericRecord> createConverter(Schema schema, Stream<Field> fields) {
        Injector<GenericRecordBuilder> injector = createRecordInjector(fields,
                field -> createInjector(schema, field.name(), field.type()));
        return value -> {
            if (value == null || (value instanceof GenericRecord
                    && ((AbstractGenericRecord) value).getClassIdentifier().equals(schema.getTypeName()))) {
                return (GenericRecord) value;
            }
            GenericRecordBuilder record = new DeserializedSchemaBoundGenericRecordBuilder(schema);
            injector.set(record, value);
            return record.build();
        };
    }

    private Injector<GenericRecordBuilder> createInjector(Schema schema, String path, QueryDataType type) {
        if (!schema.hasField(path)) {
            return (record, value) -> {
                throw QueryException.error("Field \"" + path + "\" doesn't exist in Compact schema");
            };
        }
        return createInjector0(path, type);
    }

    @SuppressWarnings("ReturnCount")
    private Injector<GenericRecordBuilder> createInjector0(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case VARCHAR:
                return (record, value) -> record.setString(path, (String) value);
            case BOOLEAN:
                return (record, value) -> record.setNullableBoolean(path, (Boolean) value);
            case TINYINT:
                return (record, value) -> record.setNullableInt8(path, (Byte) value);
            case SMALLINT:
                return (record, value) -> record.setNullableInt16(path, (Short) value);
            case INTEGER:
                return (record, value) -> record.setNullableInt32(path, (Integer) value);
            case BIGINT:
                return (record, value) -> record.setNullableInt64(path, (Long) value);
            case DECIMAL:
                return (record, value) -> record.setDecimal(path, (BigDecimal) value);
            case REAL:
                return (record, value) -> record.setNullableFloat32(path, (Float) value);
            case DOUBLE:
                return (record, value) -> record.setNullableFloat64(path, (Double) value);
            case TIME:
                return (record, value) -> record.setTime(path, (LocalTime) value);
            case DATE:
                return (record, value) -> record.setDate(path, (LocalDate) value);
            case TIMESTAMP:
                return (record, value) -> record.setTimestamp(path, (LocalDateTime) value);
            case TIMESTAMP_WITH_TIME_ZONE:
                return (record, value) -> record.setTimestampWithTimezone(path, (OffsetDateTime) value);
            case OBJECT:
                Schema fieldSchema = schemas.get(type.getObjectTypeMetadata());
                Converter<GenericRecord> converter = createConverter(fieldSchema, getFields(type));
                return (record, value) -> record.setGenericRecord(path, converter.apply(value));
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_SECOND:
            case MAP:
            case JSON:
            case ROW:
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }
}
