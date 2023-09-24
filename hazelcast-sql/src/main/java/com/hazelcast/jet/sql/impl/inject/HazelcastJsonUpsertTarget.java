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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@NotThreadSafe
class HazelcastJsonUpsertTarget extends UpsertTarget {

    @Override
    protected Converter<HazelcastJsonValue> createConverter(Stream<Field> fields) {
        List<String> fieldNames = new ArrayList<>();
        Injector<JsonObject> injector = createRecordInjector(fields, field -> {
            fieldNames.add(field.name());
            return createInjector(field.name(), field.type());
        });
        return value -> {
            if (value == null || value instanceof HazelcastJsonValue) { // Checking the schema is expensive
                return (HazelcastJsonValue) value;
            } else if (value instanceof JsonObject && ((JsonObject) value).names().equals(fieldNames)) {
                return new HazelcastJsonValue(value.toString());
            }
            JsonObject json = Json.object();
            injector.set(json, value);
            return new HazelcastJsonValue(json.toString());
        };
    }

    private Injector<JsonObject> createInjector(String path, QueryDataType type) {
        Injector<JsonObject> injector = createInjector0(path, type);
        return (json, value) -> {
            if (value == null) {
                json.add(path, (String) null);
            } else {
                injector.set(json, value);
            }
        };
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private Injector<JsonObject> createInjector0(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return (json, value) -> json.add(path, (boolean) value);
            case TINYINT:
                return (json, value) -> json.add(path, (byte) value);
            case SMALLINT:
                return (json, value) -> json.add(path, (short) value);
            case INTEGER:
                return (json, value) -> json.add(path, (int) value);
            case BIGINT:
                return (json, value) -> json.add(path, (long) value);
            case REAL:
                return (json, value) -> json.add(path, (float) value);
            case DOUBLE:
                return (json, value) -> json.add(path, (double) value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return (json, value) -> json.add(path, (String) VARCHAR.convert(value));
            case OBJECT:
                return (json, value) -> {
                    if (value instanceof JsonValue) {
                        json.add(path, (JsonValue) value);
                    } else if (value instanceof Boolean) {
                        json.add(path, (boolean) value);
                    } else if (value instanceof Byte) {
                        json.add(path, (byte) value);
                    } else if (value instanceof Short) {
                        json.add(path, (short) value);
                    } else if (value instanceof Integer) {
                        json.add(path, (int) value);
                    } else if (value instanceof Long) {
                        json.add(path, (long) value);
                    } else if (value instanceof Float) {
                        json.add(path, (float) value);
                    } else if (value instanceof Double) {
                        json.add(path, (double) value);
                    } else {
                        json.add(path, (String) VARCHAR.convert(value));
                    }
                };
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }
}
