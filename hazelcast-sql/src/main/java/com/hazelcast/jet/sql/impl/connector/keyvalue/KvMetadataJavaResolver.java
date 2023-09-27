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
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_JAVA_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.flatMap;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getSchemaId;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getTableFields;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static java.util.Map.entry;

/**
 * A utility for key-value connectors that use Java serialization
 * ({@link java.io.Serializable}) to resolve fields.
 */
public final class KvMetadataJavaResolver implements KvMetadataResolver {

    public static final KvMetadataJavaResolver INSTANCE = new KvMetadataJavaResolver();

    private KvMetadataJavaResolver() { }

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
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);

        Class<?> typeClass = getSchemaId(fieldsByPath, KvMetadataJavaResolver::loadClass,
                () -> loadClass(options, isKey));
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(typeClass);

        if (type.getTypeFamily() != QueryDataTypeFamily.OBJECT || type.isCustomType()) {
            return userFields.isEmpty()
                    ? resolvePrimitiveField(isKey, type)
                    : resolveAndValidatePrimitiveField(isKey, fieldsByPath, type);
        } else {
            return userFields.isEmpty()
                    ? resolveObjectFields(isKey, typeClass)
                    : resolveAndValidateObjectFields(isKey, fieldsByPath, typeClass);
        }
    }

    private Stream<MappingField> resolvePrimitiveField(boolean isKey, QueryDataType type) {
        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        String name = isKey ? KEY : VALUE;
        String externalName = path.toString();

        return Stream.of(new MappingField(name, type, externalName));
    }

    private Stream<MappingField> resolveAndValidatePrimitiveField(
            boolean isKey,
            Map<QueryPath, MappingField> fieldsByPath,
            QueryDataType type
    ) {
        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        String name = isKey ? KEY : VALUE;
        String externalName = path.toString();

        MappingField userField = fieldsByPath.get(path);
        if (userField != null && !userField.name().equals(name)) {
            throw QueryException.error("Cannot rename field: '" + name + '\'');
        }
        if (userField != null && type.getTypeFamily() != userField.type().getTypeFamily()) {
            throw QueryException.error("Mismatch between declared and resolved type for field '" + userField.name() + "'");
        }
        for (MappingField field : fieldsByPath.values()) {
            if (!externalName.equals(field.externalName())) {
                throw QueryException.error("The field '" + externalName + "' is of type " + type.getTypeFamily()
                        + ", you can't map '" + field.externalName() + "' too");
            }
        }

        return fieldsByPath.values().stream();
    }

    private Stream<MappingField> resolveObjectFields(boolean isKey, Class<?> typeClass) {
        Map<String, Class<?>> fieldsInClass = FieldsUtil.resolveClass(typeClass);
        if (fieldsInClass.isEmpty()) {
            // we didn't find any non-object fields in the class, map the whole value (e.g. in java.lang.Object)
            String name = isKey ? KEY : VALUE;
            return Stream.of(new MappingField(name, QueryDataType.OBJECT, name));
        }

        return fieldsInClass.entrySet().stream().map(classField -> {
            QueryPath path = new QueryPath(classField.getKey(), isKey);
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());
            String name = classField.getKey();

            return new MappingField(name, type, path.toString());
        });
    }

    private Stream<MappingField> resolveAndValidateObjectFields(
            boolean isKey,
            Map<QueryPath, MappingField> fieldsByPath,
            Class<?> typeClass
    ) {
        for (Entry<String, Class<?>> classField : FieldsUtil.resolveClass(typeClass).entrySet()) {
            QueryPath path = new QueryPath(classField.getKey(), isKey);
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());

            MappingField userField = fieldsByPath.get(path);
            if (userField != null && type.getTypeFamily() != userField.type().getTypeFamily()) {
                throw QueryException.error("Mismatch between declared and resolved type for field '"
                        + userField.name() + "'. Declared: " + userField.type().getTypeFamily()
                        + ", resolved: " + type.getTypeFamily());
            }
        }

        return fieldsByPath.values().stream();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(resolvedFields, isKey);

        Entry<QueryDataType, Class<?>> entry = flatMap(fieldsByPath,
                type -> entry(type, loadClass(type.getObjectTypeMetadata())),
                () -> {
                    Class<?> typeClass = loadClass(options, isKey);
                    return entry(QueryDataTypeUtils.resolveTypeForClass(typeClass), typeClass);
                });
        QueryDataType type = entry.getKey();
        Class<?> typeClass = entry.getValue();

        List<TableField> fields = getTableFields(isKey, fieldsByPath, type);

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                type.getTypeFamily() == QueryDataTypeFamily.OBJECT
                        ? new PojoUpsertTargetDescriptor(typeClass)
                        : PrimitiveUpsertTargetDescriptor.INSTANCE
        );
    }

    public static Class<?> loadClass(Map<String, String> options, Boolean isKey) {
        if (isKey == null) {
            String className = options.get(OPTION_TYPE_JAVA_CLASS);
            return className != null ? loadClass(className) : null;
        }

        String formatProperty = options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
        String classNameProperty = isKey ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS;

        String className = JAVA_FORMAT.equals(formatProperty)
                ? options.get(classNameProperty)
                : JavaClassNameResolver.resolveClassName(formatProperty);

        if (className == null) {
            throw QueryException.error(classNameProperty + " is required to create Java-based mapping");
        }
        return loadClass(className);
    }

    private static Class<?> loadClass(String className) {
        try {
            return ReflectionUtils.loadClass(className);
        } catch (Exception e) {
            throw QueryException.error("Unable to load class '" + className + "'", e);
        }
    }
}
