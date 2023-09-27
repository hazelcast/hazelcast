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
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@NotThreadSafe
class JsonUpsertTarget extends UpsertTarget {
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

    @Override
    protected Converter<byte[]> createConverter(Stream<Field> fields) {
        Injector<JsonGenerator> injector = createRecordInjector(fields,
                field -> createInjector(field.name(), field.type()));
        return value -> {
            if (value == null) {
                return null;
            }
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 JsonGenerator json = JSON_FACTORY.createGenerator(baos)) {
                json.writeStartObject();
                injector.set(json, value);
                json.writeEndObject();
                json.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    private Injector<JsonGenerator> createInjector(String path, QueryDataType type) {
        InjectorEx<JsonGenerator> injector = createInjector0(path, type);
        return (json, value) -> {
            try {
                if (value == null) {
                    json.writeNullField(path);
                } else {
                    injector.set(json, value);
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        };
    }

    @SuppressWarnings("ReturnCount")
    private InjectorEx<JsonGenerator> createInjector0(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return (json, value) -> json.writeBooleanField(path, (boolean) value);
            case TINYINT:
                return (json, value) -> json.writeNumberField(path, (byte) value);
            case SMALLINT:
                return (json, value) -> json.writeNumberField(path, (short) value);
            case INTEGER:
                return (json, value) -> json.writeNumberField(path, (int) value);
            case BIGINT:
                return (json, value) -> json.writeNumberField(path, (long) value);
            case REAL:
                return (json, value) -> json.writeNumberField(path, (float) value);
            case DOUBLE:
                return (json, value) -> json.writeNumberField(path, (double) value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return (json, value) -> json.writeStringField(path, (String) VARCHAR.convert(value));
            case OBJECT:
                return (json, value) -> {
                    json.writeFieldName(path);
                    if (value instanceof TreeNode) {
                        json.writeTree((TreeNode) value);
                    } else if (value instanceof Map) {
                        json.writeObject(value);
                    } else if (value instanceof Boolean) {
                        json.writeBoolean((boolean) value);
                    } else if (value instanceof Byte) {
                        json.writeNumber((byte) value);
                    } else if (value instanceof Short) {
                        json.writeNumber((short) value);
                    } else if (value instanceof Integer) {
                        json.writeNumber((int) value);
                    } else if (value instanceof Long) {
                        json.writeNumber((long) value);
                    } else if (value instanceof Float) {
                        json.writeNumber((float) value);
                    } else if (value instanceof Double) {
                        json.writeNumber((double) value);
                    } else {
                        json.writeString((String) VARCHAR.convert(value));
                    }
                };
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }
}
