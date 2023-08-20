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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.connector.map.MetadataPortableResolver.PORTABLE_TO_SQL;

public final class TypesUtils {
    private TypesUtils() { }

    public static QueryDataType convertTypeToQueryDataType(final Type rootType, final RelationsStorage relationsStorage) {
        return convertTypeToQueryDataTypeInt(rootType.name(), rootType, relationsStorage, new HashMap<>());
    }

    public static Type convertPortableClassToType(
            final String name,
            final ClassDefinition classDef,
            final TableResolverImpl tableResolver
    ) {
        final Type type = new Type();
        type.setName(name);
        type.setKind(TypeKind.PORTABLE);
        type.setPortableFactoryId(classDef.getFactoryId());
        type.setPortableClassId(classDef.getClassId());
        type.setPortableVersion(classDef.getVersion());

        final List<Type.TypeField> fields = new ArrayList<>();
        for (int i = 0; i < classDef.getFieldCount(); i++) {
            final FieldDefinition portableField = classDef.getField(i);
            final Type.TypeField typeField = new Type.TypeField();
            typeField.setName(portableField.getName());

            final QueryDataType queryDataType;
            if (portableField.getType().equals(FieldType.PORTABLE)) {
                queryDataType = toQueryDataTypeRef(tableResolver.getTypes()
                                .stream()
                                .filter(t -> t.getKind().equals(TypeKind.PORTABLE))
                                .filter(t -> t.getPortableFactoryId().equals(portableField.getFactoryId()))
                                .filter(t -> t.getPortableClassId().equals(portableField.getClassId()))
                                .filter(t -> t.getPortableVersion().equals(portableField.getVersion()))
                                .findFirst()
                                .orElseThrow(() -> QueryException.error("Type with Portable IDs "
                                        + portableField.getPortableId() + " does not exist.")));

            } else {
                queryDataType = PORTABLE_TO_SQL.getOrDefault(portableField.getType());
            }

            typeField.setQueryDataType(queryDataType);
            fields.add(typeField);
        }
        type.setFields(fields);

        return type;
    }

    public static QueryDataType toQueryDataTypeRef(Type type) {
        switch (type.getKind()) {
            case JAVA:
                return new QueryDataType(type.name(), TypeKind.JAVA, type.getJavaClassName());
            case PORTABLE:
                return new QueryDataType(type.name(), TypeKind.PORTABLE, new PortableId(
                        type.getPortableFactoryId(),
                        type.getPortableClassId(),
                        type.getPortableVersion()
                ).toString());
            case COMPACT:
                return new QueryDataType(type.name(), TypeKind.COMPACT, type.getCompactTypeName());
            default:
                throw new UnsupportedOperationException("Not implemented yet.");
        }
    }

    public static Type convertJavaClassToType(
            final String name,
            final List<TypeDefinitionColumn> columns,
            final Class<?> typeClass
    ) {
        final Map<String, QueryDataType> userColumnsMap = columns.stream()
                .collect(Collectors.toMap(TypeDefinitionColumn::name, TypeDefinitionColumn::dataType));

        final Type type = new Type();
        type.setName(name);
        type.setKind(TypeKind.JAVA);
        type.setJavaClassName(typeClass.getName());

        final List<Type.TypeField> fields = new ArrayList<>();
        for (final Map.Entry<String, Class<?>> entry : FieldsUtil.resolveClass(typeClass).entrySet()) {
            final QueryDataType queryDataType;
            if (isUserClass(entry.getValue())) {
                if (entry.getValue().getName().equals(type.getJavaClassName())) {
                    queryDataType = toQueryDataTypeRef(type);
                } else {
                    queryDataType = userColumnsMap.get(entry.getKey()) != null
                            ? userColumnsMap.get(entry.getKey())
                            : QueryDataType.OBJECT;
                }
            } else {
                queryDataType = QueryDataTypeUtils.resolveTypeForClass(entry.getValue());
            }

            fields.add(new Type.TypeField(entry.getKey(), queryDataType));
        }
        type.setFields(fields);

        return type;
    }

    public static void enrichMappingFieldType(
            final TypeKind mappingTypeKind,
            final MappingField field,
            final RelationsStorage relationsStorage
    ) {
        if (!field.type().isCustomType()) {
            return;
        }
        final Type type = relationsStorage.getType(field.type().getObjectTypeName());
        if (type == null) {
            throw QueryException.error("Non existing type found in the mapping: "
                    + field.type().getObjectTypeName());
        }

        if (!mappingTypeKind.equals(type.getKind())) {
            throw QueryException.error("Can not use Type " + type.name() + "["
                    + type.getKind() + "] with " + mappingTypeKind + " mapping.");
        }

        final QueryDataType resolved = convertTypeToQueryDataType(type, relationsStorage);
        field.setType(resolved);
    }

    public static TypeKind formatToTypeKind(String format) {
        if (format == null) {
            return TypeKind.NONE;
        }

        switch (format) {
            case SqlConnector.JAVA_FORMAT:
                return TypeKind.JAVA;
            case SqlConnector.PORTABLE_FORMAT:
                return TypeKind.PORTABLE;
            case SqlConnector.COMPACT_FORMAT:
                return TypeKind.COMPACT;
            default:
                return TypeKind.NONE;
        }
    }

    /**
     * If `type` is null, `typeName` will be used to look it up from the storage.
     */
    private static QueryDataType convertTypeToQueryDataTypeInt(
            @Nonnull final String typeName,
            @Nullable Type type,
            @Nonnull final RelationsStorage relationsStorage,
            @Nonnull final Map<String, QueryDataType> seen
    ) {
        QueryDataType convertedType = seen.get(typeName);
        if (convertedType != null) {
            return convertedType;
        }

        if (type == null) {
            type = relationsStorage.getType(typeName);
        }

        if (type == null) {
            throw QueryException.error("Encountered type '" + typeName + "', which doesn't exist");
        }

        // At this point the `convertedType` lacks fields. We put it to the `seen` map for the purpose of resolving
        // cyclic references, we'll add the fields later below.
        convertedType = toQueryDataTypeRef(type);
        seen.putIfAbsent(type.name(), convertedType);

        for (Type.TypeField field : type.getFields()) {
            QueryDataType queryDataType;
            if (field.getQueryDataType().isCustomType()) {
                queryDataType = convertTypeToQueryDataTypeInt(field.getQueryDataType().getObjectTypeName(),
                        null, relationsStorage, seen);
            } else {
                queryDataType = field.getQueryDataType();
            }

            convertedType.getObjectFields().add(
                    new QueryDataType.QueryDataTypeField(field.getName(), queryDataType));
        }

        return convertedType;
    }

    private static boolean isUserClass(Class<?> clazz) {
        return !clazz.isPrimitive() && !clazz.getPackage().getName().startsWith("java.");
    }
}
