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

package com.hazelcast.sql.impl.schema.map.options;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS;

// TODO: deduplicate with MapSampleMetadataResolver
public class PojoMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_GET_FACTORY_ID = "getFactoryId";
    private static final String METHOD_GET_CLASS_ID = "getClassId";
    private static final String METHOD_GET_CLASS_VERSION = "getVersion";

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String className = options.get(isKey ? TO_KEY_CLASS : TO_VALUE_CLASS);
        if (className != null) {
            Class<?> clazz = loadClass(className);
            return resolveClass(clazz, isKey);
        }
        return null;
    }

    private static Class<?> loadClass(String className) {
        try {
            return ClassLoaderUtil.loadClass(null, className);
        } catch (ClassNotFoundException e) {
            throw sneakyThrow(e);
        }
    }

    private static MapOptionsMetadata resolveClass(Class<?> clazz, boolean isKey) {
        Map<String, TableField> fields = new TreeMap<>();

        // Extract fields from non-primitive type.
        QueryDataType topType = QueryDataTypeUtils.resolveTypeForClass(clazz);

        if (topType == QueryDataType.OBJECT) {
            // Add public getters.
            for (Method method : clazz.getMethods()) {
                String methodName = extractAttributeNameFromMethod(clazz, method);

                if (methodName == null) {
                    continue;
                }

                QueryDataType methodType = QueryDataTypeUtils.resolveTypeForClass(method.getReturnType());

                fields.putIfAbsent(
                        methodName,
                        new MapTableField(methodName, methodType, false, new QueryPath(methodName, isKey))
                );
            }

            // Add public fields.
            Class<?> currentClass = clazz;

            while (currentClass != Object.class) {
                for (Field field : currentClass.getDeclaredFields()) {
                    if (!Modifier.isPublic(field.getModifiers())) {
                        continue;
                    }

                    String fieldName = field.getName();
                    QueryDataType fieldType = QueryDataTypeUtils.resolveTypeForClass(field.getType());

                    fields.putIfAbsent(
                            fieldName,
                            new MapTableField(fieldName, fieldType, false, new QueryPath(fieldName, isKey))
                    );
                }

                currentClass = currentClass.getSuperclass();
            }
        }

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        fields.put(topName, new MapTableField(topName, topType, !fields.isEmpty(), topPath));

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PojoUpsertTargetDescriptor(clazz.getName()),
                new LinkedHashMap<>(fields)
        );
    }

    private static String extractAttributeNameFromMethod(Class<?> clazz, Method method) {
        if (skipMethod(clazz, method)) {
            return null;
        }

        String fieldNameWithWrongCase;

        String methodName = method.getName();
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

    @SuppressWarnings({"RedundantIfStatement", "checkstyle:npathcomplexity"})
    private static boolean skipMethod(Class<?> clazz, Method method) {
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

        // Skip getFactoryId(), getClassId() and getVersion() from Portable and IdentifiedDataSerializable.
        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID)
                || methodName.equals(METHOD_GET_CLASS_ID)
                || methodName.equals(METHOD_GET_CLASS_VERSION)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return true;
            }
        }

        return false;
    }
}
