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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@NotThreadSafe
class JsonUpsertTarget extends UpsertTarget {
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonGenerator generator;

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        ConsumerEx<Object> injector = createInjector0(path, type);
        return value -> {
            try {
                if (value == null) {
                    generator.writeNullField(path);
                } else {
                    injector.accept(value);
                }
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    @SuppressWarnings("ReturnCount")
    private ConsumerEx<Object> createInjector0(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return value -> generator.writeBooleanField(path, (boolean) value);
            case TINYINT:
                return value -> generator.writeNumberField(path, (byte) value);
            case SMALLINT:
                return value -> generator.writeNumberField(path, (short) value);
            case INTEGER:
                return value -> generator.writeNumberField(path, (int) value);
            case BIGINT:
                return value -> generator.writeNumberField(path, (long) value);
            case REAL:
                return value -> generator.writeNumberField(path, (float) value);
            case DOUBLE:
                return value -> generator.writeNumberField(path, (double) value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return value -> generator.writeStringField(path, (String) VARCHAR.convert(value));
            case OBJECT:
                return value -> {
                    generator.writeFieldName(path);
                    if (value instanceof TreeNode) {
                        generator.writeTree((TreeNode) value);
                    } else if (value instanceof Map) {
                        generator.writeObject(value);
                    } else if (value instanceof Boolean) {
                        generator.writeBoolean((boolean) value);
                    } else if (value instanceof Byte) {
                        generator.writeNumber((byte) value);
                    } else if (value instanceof Short) {
                        generator.writeNumber((short) value);
                    } else if (value instanceof Integer) {
                        generator.writeNumber((int) value);
                    } else if (value instanceof Long) {
                        generator.writeNumber((long) value);
                    } else if (value instanceof Float) {
                        generator.writeNumber((float) value);
                    } else if (value instanceof Double) {
                        generator.writeNumber((double) value);
                    } else {
                        generator.writeString((String) VARCHAR.convert(value));
                    }
                };
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    @Override
    public void init() {
        baos.reset();
        try {
            generator = JSON_FACTORY.createGenerator(baos);
            generator.writeStartObject();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public Object conclude() {
        try {
            generator.writeEndObject();
            generator.close();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return baos.toByteArray();
    }
}
