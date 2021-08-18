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

    private final String typeName;

    private GenericRecordBuilder builder;

    CompactUpsertTarget(@Nonnull String typeName) {
        this.typeName = typeName;
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }
        switch (type.getTypeFamily()) {
            case VARCHAR:
                return value -> builder.setString(path, (String) value);
            case BOOLEAN:
                return value -> {
                    ensureNotNull(value);
                    builder.setBoolean(path, (Boolean) value);
                };
            case TINYINT:
                return value -> {
                    ensureNotNull(value);
                    builder.setByte(path, (Byte) value);
                };
            case SMALLINT:
                return value -> {
                    ensureNotNull(value);
                    builder.setShort(path, (Short) value);
                };
            case INTEGER:
                return value -> {
                    ensureNotNull(value);
                    builder.setInt(path, (Integer) value);
                };
            case BIGINT:
                return value -> {
                    ensureNotNull(value);
                    builder.setLong(path, (Long) value);
                };
            case DECIMAL:
                return value -> builder.setDecimal(path, (BigDecimal) value);
            case REAL:
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
            case TIMESTAMP_WITH_TIME_ZONE:
                return value -> {
                    ensureNotNull(value);
                    builder.setTimestampWithTimezone(path, (OffsetDateTime) value);
                };
            case OBJECT:
                return value -> {
                    //This is a workaround until array types introduced in the sql.
                    if (value instanceof Boolean) {
                        builder.setBoolean(path, (boolean) value);
                    } else if (value instanceof Byte) {
                        builder.setByte(path, (byte) value);
                    } else if (value instanceof Short) {
                        builder.setShort(path, (short) value);
                    } else if (value instanceof Character) {
                        builder.setChar(path, (char) value);
                    } else if (value instanceof Integer) {
                        builder.setInt(path, (int) value);
                    } else if (value instanceof Long) {
                        builder.setLong(path, (long) value);
                    } else if (value instanceof Float) {
                        builder.setFloat(path, (float) value);
                    } else if (value instanceof Double) {
                        builder.setDouble(path, (double) value);
                    } else if (value instanceof BigDecimal) {
                        builder.setDecimal(path, (BigDecimal) value);
                    } else if (value instanceof String) {
                        builder.setString(path, (String) value);
                    } else if (value instanceof LocalTime) {
                        builder.setTime(path, (LocalTime) value);
                    } else if (value instanceof LocalDate) {
                        builder.setDate(path, (LocalDate) value);
                    } else if (value instanceof LocalDateTime) {
                        builder.setTimestamp(path, (LocalDateTime) value);
                    } else if (value instanceof OffsetDateTime) {
                        builder.setTimestampWithTimezone(path, (OffsetDateTime) value);
                    } else if (value instanceof boolean[]) {
                        builder.setBooleanArray(path, (boolean[]) value);
                    } else if (value instanceof byte[]) {
                        builder.setByteArray(path, (byte[]) value);
                    } else if (value instanceof short[]) {
                        builder.setShortArray(path, (short[]) value);
                    } else if (value instanceof char[]) {
                        builder.setCharArray(path, (char[]) value);
                    } else if (value instanceof int[]) {
                        builder.setIntArray(path, (int[]) value);
                    } else if (value instanceof long[]) {
                        builder.setLongArray(path, (long[]) value);
                    } else if (value instanceof float[]) {
                        builder.setFloatArray(path, (float[]) value);
                    } else if (value instanceof double[]) {
                        builder.setDoubleArray(path, (double[]) value);
                    } else if (value instanceof BigDecimal[]) {
                        builder.setDecimalArray(path, (BigDecimal[]) value);
                    } else if (value instanceof String[]) {
                        builder.setStringArray(path, (String[]) value);
                    } else if (value instanceof LocalTime[]) {
                        builder.setTimeArray(path, (LocalTime[]) value);
                    } else if (value instanceof LocalDate[]) {
                        builder.setDateArray(path, (LocalDate[]) value);
                    } else if (value instanceof LocalDateTime[]) {
                        builder.setTimestampArray(path, (LocalDateTime[]) value);
                    } else if (value instanceof OffsetDateTime[]) {
                        builder.setTimestampWithTimezoneArray(path, (OffsetDateTime[]) value);
                    } else if (value instanceof GenericRecord[]) {
                        builder.setGenericRecordArray(path, (GenericRecord[]) value);
                    } else {
                        builder.setGenericRecord(path, (GenericRecord) value);
                    }

                };
            default:
                throw QueryException.error(type + " type is not supported by Compact format!");
        }
    }

    @Override
    public void init() {
        this.builder = GenericRecordBuilder.compact(typeName);
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
