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
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.connector.SqlConnector.POJO_SERIALIZATION_FORMAT;
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
    public String supportedFormat() {
        return POJO_SERIALIZATION_FORMAT;
    }

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String classNameProperty = isKey ? TO_KEY_CLASS : TO_VALUE_CLASS;
        String className = options.get(classNameProperty);

        if (className != null) {
            Class<?> clazz = loadClass(className);
            return resolveClass(clazz, isKey);
        }

        throw QueryException.error("Unable to resolve table metadata. Missing '" + classNameProperty + "' option");
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
            Class<?> clazz,
            boolean isKey
    ) {
        Map<String, String> typeNamesByFields = new HashMap<>();
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        for (Method method : clazz.getMethods()) {
            BiTuple<String, Class<?>> property = extractProperty(clazz, method);
            if (property == null) {
                continue;
            }

            String propertyName = property.element1();
            Class<?> propertyClass = property.element2();
            TableField tableField = toField(propertyName, propertyClass, isKey);

            typeNamesByFields.putIfAbsent(propertyName, propertyClass.getName());
            fields.putIfAbsent(propertyName, tableField);
        }

        Class<?> currentClass = clazz;
        while (currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (skipField(field)) {
                    continue;
                }

                String fieldName = field.getName();
                Class<?> fieldClass = field.getType();
                TableField tableField = toField(fieldName, fieldClass, isKey);

                typeNamesByFields.putIfAbsent(fieldName, fieldClass.getName());
                fields.putIfAbsent(fieldName, tableField);
            }
            currentClass = currentClass.getSuperclass();
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByFields),
                new LinkedHashMap<>(fields)
        );
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount", "checkstyle:NPathComplexity"})
    private static BiTuple<String, Class<?>> extractProperty(Class<?> clazz, Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return null;
        }

        if (Modifier.isStatic(method.getModifiers())) {
            return null;
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return null;
        }

        if (method.getParameterCount() != 0) {
            return null;
        }

        if (method.getDeclaringClass() == Object.class) {
            return null;
        }

        String methodName = method.getName();
        if (methodName.equals(METHOD_GET_FACTORY_ID)
                || methodName.equals(METHOD_GET_CLASS_ID)
                || methodName.equals(METHOD_GET_CLASS_VERSION)) {
            if (IdentifiedDataSerializable.class.isAssignableFrom(clazz) || Portable.class.isAssignableFrom(clazz)) {
                return null;
            }
        }

        String propertyName = extractPropertyName(method);
        if (propertyName == null) {
            return null;
        }

        Class<?> propertyClass = method.getReturnType();
        if (extractSetter(clazz, propertyName, propertyClass) == null) {
            return null;
        }

        return BiTuple.of(propertyName, propertyClass);
    }

    private static String extractPropertyName(Method method) {
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

    // TODO: extract to util class ???
    public static Field extractField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }

        if (skipField(field)) {
            return null;
        }

        return field;
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean skipField(Field field) {
        if (!Modifier.isPublic(field.getModifiers())) {
            return true;
        }

        return false;
    }

    private static MapTableField toField(String name, Class<?> clazz, boolean isKey) {
        return new MapTableField(name, QueryDataTypeUtils.resolveTypeForClass(clazz), false, new QueryPath(name, isKey));
    }
}
