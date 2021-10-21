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

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
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

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class CompactUpsertTarget implements UpsertTarget {

    private final Schema schema;

    private GenericRecordBuilder builder;

    CompactUpsertTarget(@Nonnull Schema schema) {
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType queryDataType) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }
        boolean hasField = schema.hasField(path);
        if (!hasField) {
            return value -> {
                throw QueryException.error("Unable to inject a non-null value to \"" + path + "\"");
            };
        }

        FieldKind kind = schema.getField(path).getKind();
        switch (kind) {
            case STRING:
                return value -> builder.setString(path, (String) value);
            case BOOLEAN:
                return value -> {
                    ensureNotNull(value);
                    builder.setBoolean(path, (Boolean) value);
                };
            case BYTE:
                return value -> {
                    ensureNotNull(value);
                    builder.setByte(path, (Byte) value);
                };
            case SHORT:
                return value -> {
                    ensureNotNull(value);
                    builder.setShort(path, (Short) value);
                };
            case INT:
                return value -> {
                    ensureNotNull(value);
                    builder.setInt(path, (Integer) value);
                };
            case LONG:
                return value -> {
                    ensureNotNull(value);
                    builder.setLong(path, (Long) value);
                };
            case DECIMAL:
                return value -> builder.setDecimal(path, (BigDecimal) value);
            case FLOAT:
                return value -> {
                    ensureNotNull(value);
                    builder.setFloat(path, (Float) value);
                };
            case DOUBLE:
                return value -> {
                    ensureNotNull(value);
                    builder.setDouble(path, (Double) value);
                };
            case TIME:
                return value -> {
                    ensureNotNull(value);
                    builder.setTime(path, (LocalTime) value);
                };
            case DATE:
                return value -> {
                    ensureNotNull(value);
                    builder.setDate(path, (LocalDate) value);
                };
            case TIMESTAMP:
                return value -> {
                    ensureNotNull(value);
                    builder.setTimestamp(path, (LocalDateTime) value);
                };
            case TIMESTAMP_WITH_TIMEZONE:
                return value -> {
                    ensureNotNull(value);
                    builder.setTimestampWithTimezone(path, (OffsetDateTime) value);
                };
            default:
                throw QueryException.error(kind + " kind is not supported in SQL with Compact format!");
        }
    }

    @Override
    public void init() {
        this.builder = GenericRecordBuilder.compact(schema.getTypeName());
    }

    @Override
    public Object conclude() {
        GenericRecord record = builder.build();
        builder = null;
        return record;
    }

    private static void ensureNotNull(Object value) {
        if (value == null) {
            throw QueryException.error("Cannot set NULL to a primitive field");
        }
    }
}
