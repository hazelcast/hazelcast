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
import com.hazelcast.internal.serialization.impl.compact.DeserializedSchemaBoundGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class CompactUpsertTarget extends UpsertTarget {
    private final Schema schema;

    private GenericRecordBuilder record;

    CompactUpsertTarget(Schema schema, InternalSerializationService serializationService) {
        super(serializationService);
        this.schema = schema;
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            if (type.isCustomType()) {
                Injector<GenericRecordBuilder> injector = createRecordInjector(type, this::createInjector0);
                return value -> {
                    if (value != null) {
                        injector.set(record, value);
                    }
                };
            } else {
                return FAILING_TOP_LEVEL_INJECTOR;
            }
        }
        if (!schema.hasField(path)) {
            return value -> {
                throw QueryException.error("Field \"" + path + "\" doesn't exist in Compact Schema");
            };
        }
        Injector<GenericRecordBuilder> injector = createInjector0(path, type);
        return value -> injector.set(record, value);
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
                Injector<GenericRecordBuilder> injector = createRecordInjector(type, this::createInjector0);
                return (record, value) -> {
                    if (value == null) {
                        record.setGenericRecord(path, null);
                        return;
                    }
                    GenericRecordBuilder nestedRecord = GenericRecordBuilder.compact(type.getObjectTypeMetadata());
                    injector.set(nestedRecord, value);
                    record.setGenericRecord(path, nestedRecord.build());
                };
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_SECOND:
            case MAP:
            case JSON:
            case ROW:
            default:
                throw QueryException.error("Unsupported upsert type: " + type);
        }
    }

    @Override
    public void init() {
        record = new DeserializedSchemaBoundGenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        GenericRecord record = this.record.build();
        this.record = null;
        return record;
    }
}
