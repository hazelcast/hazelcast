/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map.journal;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.connector.keyvalue.JavaClassNameResolver;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

class JavaResolver implements IMapJournalMetadataResolver {

    static final JavaResolver INSTANCE = new JavaResolver();

    @Override
    public Stream<String> supportedFormats() {
        return Stream.concat(Stream.of(JAVA_FORMAT), JavaClassNameResolver.formats());
    }

    @Override
    public List<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> cls = loadClass(isKey, options);
        return resolveFields(isKey, userFields, cls);
    }

    public List<MappingField> resolveFields(
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

    private List<MappingField> resolvePrimitiveSchema(
            boolean isKey,
            List<MappingField> userFields,
            QueryDataType type
    ) {
        String[] names = isKey ? new String[] {"key"} : new String[] {"oldValue", "newValue"};

        List<MappingField> result = new ArrayList<>();
        for (String name : names) {
            result.add(new MappingField(name, type, name));
        }
        return result;
    }

    private List<MappingField> resolveObjectSchema(
            boolean isKey,
            List<MappingField> userFields,
            Class<?> clazz
    ) {
        throw new RuntimeException("Not implemented");
    }


    @Override
    public Metadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = loadClass(isKey, options);
        return resolveMetadata(isKey, resolvedFields, clazz);
    }

    private Metadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Class<?> clazz
    ) {
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);

        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveMetadata(isKey, resolvedFields, null);
        } else {
            return resolveObjectMetadata(isKey, resolvedFields, null, clazz);
        }
    }

    private Metadata resolvePrimitiveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<QueryPath, MappingField> externalFieldsByPath) {
        String[] names = isKey ? new String[]{ "key" } : new String[]{ "oldValue", "newValue" };

        List<TableField> tableFields = new ArrayList<>();
        for (String name : names) {
            QueryDataType type = resolvedFields
                    .stream()
                    .filter(m -> m.externalName().equals(name))
                    .findFirst()
                    .map(MappingField::type)
                    .get();
            tableFields.add(new TableField(name, type, false));
        }
        return new Metadata(
                tableFields,
                isKey ? GenericQueryTargetDescriptor.DEFAULT : null,
                !isKey ? GenericQueryTargetDescriptor.DEFAULT : null
        );
    }

    private Metadata resolveObjectMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<QueryPath, MappingField> externalFieldsByPath,
            Class<?> clazz
    ) {
        throw new RuntimeException("Not implemented");
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
