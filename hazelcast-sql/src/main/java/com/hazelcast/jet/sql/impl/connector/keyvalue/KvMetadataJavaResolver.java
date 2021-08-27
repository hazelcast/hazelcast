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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;

/**
 * A utility for key-value connectors that use Java serialization ({@link
 * java.io.Serializable}) to resolve fields.
 */
public final class KvMetadataJavaResolver implements KvMetadataResolver {

    public static final KvMetadataJavaResolver INSTANCE = new KvMetadataJavaResolver();

    private KvMetadataJavaResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.concat(Stream.of(JAVA_FORMAT), JavaClassNameResolver.formats());
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = loadClass(isKey, options);
        return resolveFields(isKey, userFields, clazz);
    }

    public Stream<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Class<?> clazz
    ) {
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveSchema(isKey, userFields, type);
        } else {
            return resolveObjectSchema(isKey, userFields, clazz);
        }
    }

    private Stream<MappingField> resolvePrimitiveSchema(
            boolean isKey,
            List<MappingField> userFields,
            QueryDataType type
    ) {
        return userFields.isEmpty()
                ? resolvePrimitiveField(isKey, type)
                : resolveAndValidatePrimitiveField(isKey, userFields, type);
    }

    private Stream<MappingField> resolvePrimitiveField(boolean isKey, QueryDataType type) {
        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        String name = isKey ? KEY : VALUE;
        String externalName = path.toString();

        return Stream.of(new MappingField(name, type, externalName));
    }

    private Stream<MappingField> resolveAndValidatePrimitiveField(
            boolean isKey,
            List<MappingField> userFields,
            QueryDataType type
    ) {
        Map<QueryPath, MappingField> userFieldsByPath = extractFields(userFields, isKey);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        String name = isKey ? KEY : VALUE;
        String externalName = path.toString();

        MappingField userField = userFieldsByPath.get(path);
        if (userField != null && !userField.name().equals(name)) {
            throw QueryException.error("Cannot rename field: '" + name + '\'');
        }
        if (userField != null && !type.getTypeFamily().equals(userField.type().getTypeFamily())) {
            throw QueryException.error("Mismatch between declared and resolved type for field '" + userField.name() + "'");
        }
        for (MappingField field : userFieldsByPath.values()) {
            if (!externalName.equals(field.externalName())) {
                throw QueryException.error("The field '" + externalName + "' is of type " + type.getTypeFamily()
                        + ", you can't map '" + field.externalName() + "' too");
            }
        }

        return userFieldsByPath.values().stream();
    }

    private Stream<MappingField> resolveObjectSchema(boolean isKey, List<MappingField> userFields, Class<?> clazz) {
        return userFields.isEmpty()
                ? resolveObjectFields(isKey, clazz)
                : resolveAndValidateObjectFields(isKey, userFields, clazz);
    }

    private Stream<MappingField> resolveObjectFields(boolean isKey, Class<?> clazz) {
        Map<String, Class<?>> fieldsInClass = FieldsUtil.resolveClass(clazz);
        if (fieldsInClass.isEmpty()) {
            // we didn't find any non-object fields in the class, map the whole value (e.g. in java.lang.Object)
            String name = isKey ? KEY : VALUE;
            return Stream.of(new MappingField(name, QueryDataType.OBJECT, name));
        }

        return fieldsInClass.entrySet().stream()
                .map(classField -> {
                    QueryPath path = new QueryPath(classField.getKey(), isKey);
                    QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());
                    String name = classField.getKey();

                    return new MappingField(name, type, path.toString());
                });
    }

    private Stream<MappingField> resolveAndValidateObjectFields(
            boolean isKey,
            List<MappingField> userFields,
            Class<?> clazz
    ) {
        Map<QueryPath, MappingField> userFieldsByPath = extractFields(userFields, isKey);
        for (Entry<String, Class<?>> classField : FieldsUtil.resolveClass(clazz).entrySet()) {
            QueryPath path = new QueryPath(classField.getKey(), isKey);
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());

            MappingField userField = userFieldsByPath.get(path);
            if (userField != null && !type.getTypeFamily().equals(userField.type().getTypeFamily())) {
                throw QueryException.error("Mismatch between declared and resolved type for field '"
                        + userField.name() + "'. Declared: " + userField.type().getTypeFamily()
                        + ", resolved: " + type.getTypeFamily());
            }
        }
        return userFieldsByPath.values().stream();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = loadClass(isKey, options);
        return resolveMetadata(isKey, resolvedFields, clazz);
    }

    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Class<?> clazz
    ) {
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        Map<QueryPath, MappingField> fields = extractFields(resolvedFields, isKey);

        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveMetadata(isKey, resolvedFields, fields, type);
        } else {
            return resolveObjectMetadata(isKey, resolvedFields, fields, clazz);
        }
    }

    private KvMetadata resolvePrimitiveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<QueryPath, MappingField> fieldsByPath,
            QueryDataType type
    ) {
        List<TableField> fields = new ArrayList<>();
        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        MappingField field = fieldsByPath.get(path);
        if (field != null) {
            fields.add(new MapTableField(field.name(), field.type(), false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, type);

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );
    }

    private KvMetadata resolveObjectMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<QueryPath, MappingField> fieldsByPath,
            Class<?> clazz
    ) {
        Map<String, Class<?>> typesByNames = FieldsUtil.resolveClass(clazz);

        List<TableField> fields = new ArrayList<>();
        Map<String, String> typeNamesByPaths = new HashMap<>();
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
            if (path.getPath() != null && typesByNames.get(path.getPath()) != null) {
                typeNamesByPaths.put(path.getPath(), typesByNames.get(path.getPath()).getName());
            }
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByPaths)
        );
    }

    private Class<?> loadClass(boolean isKey, Map<String, String> options) {
        String formatProperty = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        String classNameProperty = isKey ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS;

        String className = JAVA_FORMAT.equals(formatProperty)
                ? options.get(classNameProperty)
                : JavaClassNameResolver.resolveClassName(formatProperty);

        if (className == null) {
            throw QueryException.error("Unable to resolve table metadata. Missing '" + classNameProperty + "' option");
        }

        try {
            return ReflectionUtils.loadClass(className);
        } catch (Exception e) {
            throw QueryException.error("Unable to load class: '" + className + "'", e);
        }
    }
}
