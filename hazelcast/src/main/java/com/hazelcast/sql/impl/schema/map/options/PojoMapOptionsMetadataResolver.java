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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;

// TODO: deduplicate with MapSampleMetadataResolver
public class PojoMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_PREFIX_SET = "set";

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
            return resolveClass(externalFields, clazz, isKey);
        }

        return null;
    }

    // TODO: extract to util class ???
    public static Class<?> loadClass(String className) {
        try {
            return ClassLoaderUtil.loadClass(null, className);
        } catch (ClassNotFoundException e) {
            throw QueryException.dataException(
                    format("Unable to load class \"%s\" : %s", className, e.getMessage()), e
            );
        }
    }

    private static MapOptionsMetadata resolveClass(
            List<ExternalField> externalFields,
            Class<?> clazz,
            boolean isKey
    ) {
        Map<String, String> typeNamesByFields = new HashMap<>();
        LinkedHashMap<String, QueryPath> fields = new LinkedHashMap<>();

        // TODO: validate types match ???
        for (ExternalField externalField : externalFields) {
            String fieldName = externalField.name();

            Class<?> attributeClass = extractPropertyClass(clazz, fieldName);

            if (attributeClass == null) {
                attributeClass = extractFieldClass(clazz, fieldName);
            }

            if (attributeClass == null) {
                continue;
            }

            typeNamesByFields.putIfAbsent(fieldName, attributeClass.getName());
            fields.putIfAbsent(fieldName, new QueryPath(fieldName, isKey));
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByFields),
                new LinkedHashMap<>(fields)
        );
    }

    private static Class<?> extractPropertyClass(Class<?> clazz, String propertyName) {
        Method getter = extractGetter(clazz, propertyName);
        if (getter == null) {
            return null;
        }

        Class<?> propertyClass = getter.getReturnType();

        Method setter = extractSetter(clazz, propertyName, propertyClass);
        if (setter == null) {
            return null;
        }

        return propertyClass;
    }

    private static Method extractGetter(Class<?> clazz, String propertyName) {
        String getName = METHOD_PREFIX_GET + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);
        String isName = METHOD_PREFIX_IS + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

        for (Method method : clazz.getMethods()) {
            String methodName = method.getName();
            if ((getName.equals(methodName) || isName.equals(methodName)) && isGetter(clazz, method)) {
                return method;
            }
        }

        return null;
    }

    @SuppressWarnings({"RedundantIfStatement", "checkstyle:npathcomplexity"})
    private static boolean isGetter(Class<?> clazz, Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return false;
        }

        if (method.getParameterCount() != 0) {
            return false;
        }

        if (method.getDeclaringClass() == Object.class) {
            return false;
        }

        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID)
                || methodName.equals(METHOD_GET_CLASS_ID)
                || methodName.equals(METHOD_GET_CLASS_VERSION)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return false;
            }
        }

        return true;
    }

    // TODO: extract to util class ???
    public static Method extractSetter(Class<?> clazz, String propertyName, Class<?> type) {
        String setName = METHOD_PREFIX_SET + toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

        Method method;
        try {
            method = clazz.getMethod(setName, type);
        } catch (NoSuchMethodException e) {
            return null;
        }

        if (!isSetter(method)) {
            return null;
        }

        return method;
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean isSetter(Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType != void.class && returnType != Void.class) {
            return false;
        }

        return true;
    }

    private static Class<?> extractFieldClass(Class<?> clazz, String fieldName) {
        Field field = extractField(clazz, fieldName);

        if (field == null) {
            return null;
        }

        return field.getType();
    }

    // TODO: extract to util class ???
    public static Field extractField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }

        if (!Modifier.isPublic(field.getModifiers())) {
            return null;
        }

        return field;
    }
}
