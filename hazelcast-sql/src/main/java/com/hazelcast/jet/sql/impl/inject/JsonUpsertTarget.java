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

package com.hazelcast.jet.sql.impl.inject;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.TreeNode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import tools.jackson.databind.json.JsonMapper;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class JsonUpsertTarget implements UpsertTarget {

    private static final JsonMapper JSON_MAPPER = new JsonMapper();

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
            if (value == null) {
                generator.writeNullProperty(path);
            } else {
                switch (type.getTypeFamily()) {
                    case BOOLEAN:
                        generator.writeBooleanProperty(path, (Boolean) value);
                        break;
                    case TINYINT:
                        generator.writeNumberProperty(path, (Byte) value);
                        break;
                    case SMALLINT:
                        generator.writeNumberProperty(path, (Short) value);
                        break;
                    case INTEGER:
                        generator.writeNumberProperty(path, (Integer) value);
                        break;
                    case BIGINT:
                        generator.writeNumberProperty(path, (Long) value);
                        break;
                    case REAL:
                        generator.writeNumberProperty(path, (Float) value);
                        break;
                    case DOUBLE:
                        generator.writeNumberProperty(path, (Double) value);
                        break;
                    case DECIMAL:
                    case TIME:
                    case DATE:
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_TIME_ZONE:
                    case VARCHAR:
                        generator.writeStringProperty(path, (String) QueryDataType.VARCHAR.convert(value));
                        break;
                    case OBJECT:
                        injectObject(path, value);
                        break;
                    default:
                        throw QueryException.error("Unsupported type: " + type);
                }
            }
        };
    }

    private void injectObject(String path, Object value) {
        generator.writeName(path);
        if (value == null) {
            generator.writeNull();
        } else if (value instanceof TreeNode node) {
            generator.writeTree(node);
        } else if (value instanceof Map) {
            generator.writePOJO(value);
        } else if (value instanceof Boolean b) {
            generator.writeBoolean(b);
        } else if (value instanceof Byte b) {
            generator.writeNumber(b);
        } else if (value instanceof Short s) {
            generator.writeNumber(s);
        } else if (value instanceof Integer i) {
            generator.writeNumber(i);
        } else if (value instanceof Long l) {
            generator.writeNumber(l);
        } else if (value instanceof Float f) {
            generator.writeNumber(f);
        } else if (value instanceof Double d) {
            generator.writeNumber(d);
        } else {
            generator.writeString((String) QueryDataType.VARCHAR.convert(value));
        }
    }

    @Override
    public void init() {
        baos.reset();
        generator = JSON_MAPPER.createGenerator(baos);
        generator.writeStartObject();
    }

    @Override
    public Object conclude() {
        generator.writeEndObject();
        generator.close();
        return baos.toByteArray();
    }
}
