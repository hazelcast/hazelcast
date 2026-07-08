/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ParsingException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import tools.jackson.core.JacksonException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:MissingSwitchDefault")
public final class StructToMap {
    private static final Set<String> TIME_SCHEMAS = Set.of(
            io.debezium.time.Time.SCHEMA_NAME,
            Time.LOGICAL_NAME
    );

    private StructToMap() {
    }

    public static Map<String, Object> toMap(Struct struct) {
        var result = new LinkedHashMap<String, Object>();
        for (Field field : struct.schema().fields()) {
            Object object = struct.get(field.name());
            final Schema schema = field.schema();


            object = switch (schema.type()) {
                case STRUCT -> toMap((Struct) object);
                case INT32 -> checkInt32ForDate(object, schema);
                case INT64 -> checkInt64ForDate(object, schema);
                default -> object;
            };
            result.put(field.name(), object);
        }
        return result;
    }

    private static Object checkInt32ForDate(Object object, Schema schema) {
        if (schema.name() == null) {
            return object;
        }
        if (object instanceof Integer value) {
            return switch (schema.name()) {
                case io.debezium.time.Date.SCHEMA_NAME ->
                        LocalDate.ofEpochDay(value);
                case io.debezium.time.Time.SCHEMA_NAME ->
                        Duration.ofMillis(value);
                default -> object;
            };
        }

        if (object instanceof Date value && TIME_SCHEMAS.contains(schema.name())) {
            return Duration.ofMillis(value.getTime());
        }
        return object;
    }

    private static Object checkInt64ForDate(Object object, Schema schema) {
        if (schema.name() == null) {
            return object;
        }
        if (object instanceof Long value) {
            return switch (schema.name()) {
                case io.debezium.time.Timestamp.SCHEMA_NAME ->
                        new Date(value).toInstant();
                case io.debezium.time.MicroTimestamp.SCHEMA_NAME -> {
                    long nanos = TimeUnit.MICROSECONDS.toNanos(value);
                    yield Instant.EPOCH.plusNanos(nanos);
                }
                case io.debezium.time.MicroTime.SCHEMA_NAME -> {
                    long nanos = TimeUnit.MICROSECONDS.toNanos(value);
                    yield Duration.ofNanos(nanos);
                }
                case io.debezium.time.NanoTimestamp.SCHEMA_NAME ->
                        Instant.EPOCH.plusNanos(value);
                case io.debezium.time.NanoTime.SCHEMA_NAME ->
                        Duration.ofNanos(value);
                default -> object;
            };
        }
        if (object instanceof Date value) {
            if (schema.name().equals(io.debezium.time.NanoTime.SCHEMA_NAME)) {
                return Duration.ofNanos(value.getTime());
            } else if (schema.name().equals(io.debezium.time.MicroTime.SCHEMA_NAME)) {
                long nanos = TimeUnit.MICROSECONDS.toNanos(value.getTime());
                return Duration.ofNanos(nanos);
            }
        }
        return object;
    }

    public static String toJson(Struct struct) {
        try {
            return Utils.MAPPER.writeValueAsString(toMap(struct));
        } catch (JacksonException e) {
            throw new ParsingException("Unable to parse struct", e);
        }
    }
    public static String toJson(Object o) {
        if (o instanceof Struct s) {
            return toJson(s);
        }
        if (o instanceof String s) {
            return s;
        }
        try {
            return Utils.MAPPER.writeValueAsString(o);
        } catch (JacksonException e) {
            throw new ParsingException("Unable to parse struct", e);
        }
    }
}
