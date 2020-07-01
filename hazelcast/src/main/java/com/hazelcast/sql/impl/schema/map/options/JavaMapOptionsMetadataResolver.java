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
import com.hazelcast.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;
import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

// TODO: deduplicate with MapSampleMetadataResolver
public final class JavaMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    public static final JavaMapOptionsMetadataResolver INSTANCE = new JavaMapOptionsMetadataResolver();

    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_PREFIX_SET = "set";

    private static final String METHOD_GET_FACTORY_ID = "getFactoryId";
    private static final String METHOD_GET_CLASS_ID = "getClassId";
    private static final String METHOD_GET_CLASS_VERSION = "getVersion";

    private JavaMapOptionsMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return JAVA_SERIALIZATION_FORMAT;
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

        if (className == null) {
            throw QueryException.error(format("Unable to resolve table metadata. Missing '%s' option", classNameProperty));
        }

        Class<?> clazz = loadClass(className);

        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitive(externalFields, type, isKey);
        } else {
            return resolveObject(externalFields, clazz, isKey);
        }
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

    private MapOptionsMetadata resolvePrimitive(
            List<ExternalField> externalFields,
            QueryDataType type,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath =
                extractFields(externalFields, isKey, name -> VALUE_PATH);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;

        ExternalField externalField = externalFieldsByPath.get(path);
        if (externalField != null && !externalField.type().equals(type)) {
            throw QueryException.error(
                    format("Mismatch between declared and inferred type - '%s'", externalField.name())
            );
        }
        String name = externalField == null ? (isKey ? QueryPath.KEY : QueryPath.VALUE) : externalField.name();

        TableField field = new MapTableField(name, type, false, path);

        for (ExternalField ef : externalFieldsByPath.values()) {
            if (!field.getName().equals(ef.name())) {
                throw QueryException.error(format("Unmapped field - '%s'", ef.name()));
            }
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                new LinkedHashMap<>(singletonMap(field.getName(), field))
        );
    }

    private MapOptionsMetadata resolveObject(
            List<ExternalField> externalFields,
            Class<?> clazz,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath =
                extractFields(externalFields, isKey, name -> new QueryPath(name, false));

        Map<String, String> typeNamesByPaths = new HashMap<>();
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        for (Entry<String, Class<?>> entry : resolveClass(clazz).entrySet()) {
            QueryPath path = new QueryPath(entry.getKey(), isKey);
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(entry.getValue());

            ExternalField externalField = externalFieldsByPath.get(path);
            if (externalField != null && !externalField.type().equals(type)) {
                throw QueryException.error(
                        format("Mismatch between declared and inferred type - '%s'", externalField.name())
                );
            }
            String name = externalField == null ? entry.getKey() : externalField.name();

            TableField field = new MapTableField(name, type, false, path);

            typeNamesByPaths.putIfAbsent(path.getPath(), entry.getValue().getName());
            fields.putIfAbsent(field.getName(), field);
        }

        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            String name = entry.getValue().name();
            QueryDataType type = entry.getValue().type();

            TableField field = new MapTableField(name, type, false, path);

            fields.put(field.getName(), field);
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByPaths),
                new LinkedHashMap<>(fields)
        );
    }

    private static Map<String, Class<?>> resolveClass(
            Class<?> clazz
    ) {
        Map<String, Class<?>> fields = new LinkedHashMap<>();

        for (Method method : clazz.getMethods()) {
            BiTuple<String, Class<?>> property = extractProperty(clazz, method);
            if (property == null) {
                continue;
            }
            fields.putIfAbsent(property.element1(), property.element2());
        }

        Class<?> classToInspect = clazz;
        while (classToInspect != Object.class) {
            for (Field field : classToInspect.getDeclaredFields()) {
                if (skipField(field)) {
                    continue;
                }
                fields.putIfAbsent(field.getName(), field.getType());
            }
            classToInspect = classToInspect.getSuperclass();
        }

        return fields;
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

        return toLowerCase(fieldNameWithWrongCase.charAt(0)) + fieldNameWithWrongCase.substring(1);
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
}
