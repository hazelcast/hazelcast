/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utilities to extract a list of properties from a
 * {@link Class} object using reflection
 * or from {@link ClassDefinition} of a Portable.
 * or from {@link Schema} of a Compact Serialized Object
 */
public final class FieldsUtil {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_GET_FACTORY_ID = "getFactoryId";
    private static final String METHOD_GET_CLASS_ID = "getClassId";

    private FieldsUtil() {
    }

    /**
     * Return a list of fields and their types from a {@link Class}.
     */
    @Nonnull
    public static SortedMap<String, Class<?>> resolveClass(@Nonnull Class<?> clazz) {
        SortedMap<String, Class<?>> fields = new TreeMap<>();

        // Add public getters.
        for (Method method : clazz.getMethods()) {
            String attributeName = extractAttributeNameFromMethod(clazz, method);

            if (attributeName == null) {
                continue;
            }

            fields.putIfAbsent(attributeName, method.getReturnType());
        }

        // Add public fields.
        Class<?> currentClass = clazz;

        while (currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (!Modifier.isPublic(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                fields.putIfAbsent(field.getName(), field.getType());
            }

            currentClass = currentClass.getSuperclass();
        }

        return fields;
    }

    @Nullable
    private static String extractAttributeNameFromMethod(@Nonnull Class<?> clazz, @Nonnull Method method) {
        if (skipMethod(clazz, method)) {
            return null;
        }

        String methodName = method.getName();

        String fieldNameWithWrongCase;

        if (methodName.startsWith(METHOD_PREFIX_GET) && methodName.length() > METHOD_PREFIX_GET.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_GET.length());
        } else if (methodName.startsWith(METHOD_PREFIX_IS) && methodName.length() > METHOD_PREFIX_IS.length()) {
            // Skip getters that do not return primitive boolean.
            if (method.getReturnType() != boolean.class) {
                return null;
            }

            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_IS.length());
        } else {
            return null;
        }

        return Character.toLowerCase(fieldNameWithWrongCase.charAt(0)) + fieldNameWithWrongCase.substring(1);
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean skipMethod(@Nonnull Class<?> clazz, @Nonnull Method method) {
        // Exclude non-public getters.
        if (!Modifier.isPublic(method.getModifiers())) {
            return true;
        }

        // Exclude static getters.
        if (Modifier.isStatic(method.getModifiers())) {
            return true;
        }

        // Exclude void return type.
        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return true;
        }

        // Skip methods with parameters.
        if (method.getParameterCount() != 0) {
            return true;
        }

        // Skip "getClass"
        if (method.getDeclaringClass() == Object.class) {
            return true;
        }

        // Skip getFactoryId() and getClassId() from Portable and IdentifiedDataSerializable.
        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID) || methodName.equals(METHOD_GET_CLASS_ID)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Resolve the list of fields from a schema {@link com.hazelcast.internal.serialization.impl.compact.Schema},
     * along with their {@link QueryDataType}.
     */
    @Nonnull
    public static SortedMap<String, QueryDataType> resolveCompact(@Nonnull Schema schema) {
        SortedMap<String, QueryDataType> fields = new TreeMap<>();

        // Add regular fields.
        for (String name : schema.getFieldNames()) {
            FieldKind compactKind = schema.getField(name).getKind();
            QueryDataType type = resolveType(compactKind);
            fields.putIfAbsent(name, type);
        }

        return fields;
    }

    @SuppressWarnings({"checkstyle:ReturnCount", "checkstyle:cyclomaticcomplexity"})
    @Nonnull
    private static QueryDataType resolveType(@Nonnull FieldKind kind) {
        switch (kind) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;

            case INT8:
                return QueryDataType.TINYINT;

            case INT16:
                return QueryDataType.SMALLINT;

            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;

            case STRING:
                return QueryDataType.VARCHAR;

            case INT32:
                return QueryDataType.INT;

            case INT64:
                return QueryDataType.BIGINT;

            case FLOAT32:
                return QueryDataType.REAL;

            case FLOAT64:
                return QueryDataType.DOUBLE;

            case DECIMAL:
                return QueryDataType.DECIMAL;

            case TIME:
                return QueryDataType.TIME;

            case DATE:
                return QueryDataType.DATE;

            case TIMESTAMP:
                return QueryDataType.TIMESTAMP;

            case TIMESTAMP_WITH_TIMEZONE:
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            default:
                return QueryDataType.OBJECT;
        }
    }
}
