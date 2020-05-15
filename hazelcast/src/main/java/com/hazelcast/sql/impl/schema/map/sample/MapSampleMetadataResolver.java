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

package com.hazelcast.sql.impl.schema.map.sample;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;

/**
 * Helper class that resolves a map-backed table from a key/value sample.
 */
public final class MapSampleMetadataResolver {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_GET_CLASS = "getClass";

    private MapSampleMetadataResolver() {
        // No-op.
    }

    /**
     * Resolves the metadata associated with the given key-value sample.
     *
     * @param ss Serialization service.
     * @param target Target to be analyzed.
     * @param isKey Whether the is the key or the value.
     * @return Sample metadata.
     * @throws QueryException If metadata cannot be resolved.
     */
    public static MapSampleMetadata resolve(InternalSerializationService ss, Object target, boolean isKey) {
        try {
            if (target instanceof Data) {
                Data data = (Data) target;

                if (data.isPortable()) {
                    return resolvePortable(ss.getPortableContext().lookupClassDefinition(data), isKey);
                } else if (data.isJson()) {
                    throw new UnsupportedOperationException("JSON objects are not supported.");
                } else {
                    return resolveClass(ss.toObject(data).getClass(), isKey);
                }
            } else {
                return resolveClass(target.getClass(), isKey);
            }
        } catch (Exception e) {
            throw QueryException.error("Failed to resolve " + (isKey ? "key" : "value") + " metadata: " + e.getMessage(), e);
        }
    }

    /**
     * Resolve metadata from a portable object.
     *
     * @param clazz Portable class definition.
     * @param isKey Whether this is a key.
     * @return Metadata.
     */
    private static MapSampleMetadata resolvePortable(ClassDefinition clazz, boolean isKey) {
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        fields.put(topName, new MapTableField(topName, QueryDataType.OBJECT, topPath));

        // Add regular fields.
        for (String name : clazz.getFieldNames()) {
            FieldType portableType = clazz.getFieldType(name);

            QueryDataType type = resolvePortableType(portableType);

            fields.putIfAbsent(name, new MapTableField(name, type, new QueryPath(name, isKey)));
        }

        return new MapSampleMetadata(GenericQueryTargetDescriptor.INSTANCE, fields);
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType portableType) {
        switch (portableType) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;

            case BYTE:
                return QueryDataType.TINYINT;

            case SHORT:
                return QueryDataType.SMALLINT;

            case CHAR:
            case UTF:
                return QueryDataType.VARCHAR;

            case INT:
                return QueryDataType.INT;

            case LONG:
                return QueryDataType.BIGINT;

            case FLOAT:
                return QueryDataType.REAL;

            case DOUBLE:
                return QueryDataType.DOUBLE;

            default:
                return QueryDataType.OBJECT;
        }
    }

    private static MapSampleMetadata resolveClass(Class<?> clazz, boolean isKey) {
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        QueryDataType topType = QueryDataTypeUtils.resolveTypeForClass(clazz);
        fields.put(topName, new MapTableField(topName, topType, topPath));

        // Add regular fields.
        for (Method method : clazz.getMethods()) {
            Class<?> returnType = method.getReturnType();

            if (returnType == void.class || returnType == Void.class) {
                continue;
            }

            String name = extractAttributeNameFromMethod(method);

            if (name == null) {
                continue;
            }

            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(returnType);

            fields.putIfAbsent(name, new MapTableField(name, type, new QueryPath(name, isKey)));
        }

        return new MapSampleMetadata(GenericQueryTargetDescriptor.INSTANCE, fields);
    }

    private static String extractAttributeNameFromMethod(Method method) {
        String methodName = method.getName();

        // Skip "getClass"
        if (methodName.equals(METHOD_GET_CLASS)) {
            return null;
        }

        String fieldNameWithWrongCase;

        if (methodName.startsWith(METHOD_PREFIX_GET) && methodName.length() > METHOD_PREFIX_GET.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_GET.length());
        } else if (methodName.startsWith(METHOD_PREFIX_IS) && methodName.length() > METHOD_PREFIX_IS.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_IS.length());
        } else {
            return null;
        }

        return Character.toLowerCase(fieldNameWithWrongCase.charAt(0)) + fieldNameWithWrongCase.substring(1);
    }
}
