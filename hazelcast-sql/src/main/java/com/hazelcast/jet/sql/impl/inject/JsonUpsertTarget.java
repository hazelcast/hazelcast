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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class JsonUpsertTarget implements UpsertTarget {

    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonGenerator generator;

    JsonUpsertTarget() {
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        return value -> {
            try {
                if (value == null) {
                    generator.writeNullField(path);
                } else {
                    switch (type.getTypeFamily()) {
                        case BOOLEAN:
                            generator.writeBooleanField(path, (Boolean) value);
                            break;
                        case TINYINT:
                            generator.writeNumberField(path, (Byte) value);
                            break;
                        case SMALLINT:
                            generator.writeNumberField(path, (Short) value);
                            break;
                        case INTEGER:
                            generator.writeNumberField(path, (Integer) value);
                            break;
                        case BIGINT:
                            generator.writeNumberField(path, (Long) value);
                            break;
                        case REAL:
                            generator.writeNumberField(path, (Float) value);
                            break;
                        case DOUBLE:
                            generator.writeNumberField(path, (Double) value);
                            break;
                        case DECIMAL:
                        case TIME:
                        case DATE:
                        case TIMESTAMP:
                        case TIMESTAMP_WITH_TIME_ZONE:
                        case VARCHAR:
                            generator.writeStringField(path, (String) QueryDataType.VARCHAR.convert(value));
                            break;
                        case OBJECT:
                            injectObject(path, value);
                            break;
                        default:
                            throw QueryException.error("Unsupported type: " + type);
                    }
                }
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    private void injectObject(String path, Object value) throws IOException {
        generator.writeFieldName(path);
        if (value == null) {
            generator.writeNull();
        } else if (value instanceof TreeNode) {
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
            generator.writeString((String) QueryDataType.VARCHAR.convert(value));
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
