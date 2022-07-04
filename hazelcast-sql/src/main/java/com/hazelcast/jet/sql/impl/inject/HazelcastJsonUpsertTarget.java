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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class HazelcastJsonUpsertTarget implements UpsertTarget {

    private JsonObject json;

    HazelcastJsonUpsertTarget() {
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (boolean) value);
                    }
                };
            case TINYINT:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (byte) value);
                    }
                };
            case SMALLINT:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (short) value);
                    }
                };
            case INTEGER:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (int) value);
                    }
                };
            case BIGINT:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (long) value);
                    }
                };
            case REAL:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (float) value);
                    }
                };
            case DOUBLE:
                return value -> {
                    if (value == null) {
                        json.add(path, (String) null);
                    } else {
                        json.add(path, (double) value);
                    }
                };
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return value -> json.add(path, (String) QueryDataType.VARCHAR.convert(value));
            case OBJECT:
                return createObjectInjector(path);
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    private UpsertInjector createObjectInjector(String path) {
        return value -> {
            if (value == null) {
                json.add(path, (String) null);
            } else if (value instanceof JsonValue) {
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
                json.add(path, (String) QueryDataType.VARCHAR.convert(value));
            }
        };
    }

    @Override
    public void init() {
        json = Json.object();
    }

    @Override
    public Object conclude() {
        JsonObject json = this.json;
        this.json = null;
        return new HazelcastJsonValue(json.toString());
    }
}
