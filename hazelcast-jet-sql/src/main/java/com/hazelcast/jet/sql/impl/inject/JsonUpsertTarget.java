/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class JsonUpsertTarget implements UpsertTarget {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ObjectNode json;

    JsonUpsertTarget() {
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return value -> json.put(path, (Boolean) value);
            case TINYINT:
                return value -> {
                    if (value == null) {
                        json.putNull(path);
                    } else {
                        json.put(path, (Byte) value);
                    }
                };
            case SMALLINT:
                return value -> json.put(path, (Short) value);
            case INTEGER:
                return value -> json.put(path, (Integer) value);
            case BIGINT:
                return value -> json.put(path, (Long) value);
            case REAL:
                return value -> json.put(path, (Float) value);
            case DOUBLE:
                return value -> json.put(path, (Double) value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return value -> json.put(path, (String) QueryDataType.VARCHAR.convert(value));
            case OBJECT:
                return createObjectInjector(path);
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    private UpsertInjector createObjectInjector(String path) {
        return value -> {
            if (value == null) {
                json.putNull(path);
            } else if (value instanceof JsonNode) {
                json.set(path, (JsonNode) value);
            } else if (value instanceof Boolean) {
                json.put(path, (boolean) value);
            } else if (value instanceof Byte) {
                json.put(path, (byte) value);
            } else if (value instanceof Short) {
                json.put(path, (short) value);
            } else if (value instanceof Integer) {
                json.put(path, (int) value);
            } else if (value instanceof Long) {
                json.put(path, (long) value);
            } else if (value instanceof Float) {
                json.put(path, (float) value);
            } else if (value instanceof Double) {
                json.put(path, (double) value);
            } else {
                json.put(path, (String) QueryDataType.VARCHAR.convert(value));
            }
        };
    }

    @Override
    public void init() {
        json = MAPPER.createObjectNode();
    }

    @Override
    public Object conclude() {
        ObjectNode json = this.json;
        this.json = null;
        try {
            return MAPPER.writeValueAsBytes(json);
        } catch (JsonProcessingException jpe) {
            throw sneakyThrow(jpe);
        }
    }
}
