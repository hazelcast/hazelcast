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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

// TODO: Merge into TablesStorage
public class TypesStorage extends TablesStorage {
    final InternalSerializationService iss;

    public TypesStorage(final NodeEngine nodeEngine) {
        super(nodeEngine);
        this.iss = (InternalSerializationService) nodeEngine.getSerializationService();
    }

    public Type getTypeByClass(final Class<?> typeClass) {
        return super.getAllTypes().stream()
                .filter(type -> type.getJavaClassName().equals(typeClass.getName()))
                .findFirst()
                .orElse(null);
    }

    public Type getTypeByPortableClass(final int factoryId, final int classId, final int versionId) {
        return super.getAllTypes().stream()
                .filter(type -> type.getKind().equals(TypeKind.PORTABLE))
                .filter(type -> type.getPortableFactoryId() == factoryId
                        && type.getPortableClassId() == classId
                        && type.getPortableVersion() == versionId)
                .findFirst()
                .orElse(null);
    }

    public boolean registerType(final String name, final Class<?> typeClass, final boolean onlyIfAbsent) {
        if ((getTypeByClass(typeClass) != null || getType(name) != null) && onlyIfAbsent) {
            return false;
        }

        final Type type = new Type();
        type.setName(name);
        type.setJavaClassName(typeClass.getName());

        if (typeClass.isAssignableFrom(GenericRecord.class)) {
            type.setFields(getFieldsFromGenericRecord(typeClass));
        } else {
            type.setFields(getFieldsFromJavaClass(typeClass, type));
        }

        boolean result = true;
        if (onlyIfAbsent) {
            result = putIfAbsent(name, type);
        } else {
            put(name, type);
        }

        fixTypeReferences(type);

        return result;
    }

    public boolean registerType(String name, ClassDefinition classDef) {
        Type type = TypesUtils.convertPortableClassToType(name, classDef, this);
        put(name, type);

        return true;
    }

    public boolean register(String name, Type type) {
        put(name, type);
        return true;
    }

    public boolean register(final String name, final Type type, final boolean onlyIfAbsent) {
        boolean result = true;
        if (onlyIfAbsent) {
            result = putIfAbsent(name, type);
        } else {
            put(name, type);
        }

        fixTypeReferences(type);

        return result;
    }

    public Schema getTypeCompactSchema(String typeName, Long fingerprint) {
        final Type type = getType(typeName);
        final TreeMap<String, FieldDescriptor> fieldDefinitions = new TreeMap<>();
        for (int i = 0; i < type.getFields().size(); i++) {
            final Type.TypeField typeField = type.getFields().get(i);
            fieldDefinitions.put(typeField.getName(), new FieldDescriptor(
                    typeField.getName(),
                    QueryDataTypeUtils.resolveCompactType(typeField.getQueryDataType())
            ));
        }
        final Schema schema = new Schema(typeName, fieldDefinitions);

        assert schema.getSchemaId() == fingerprint;

        return schema;
    }

    private void fixTypeReferences(final Type addedType) {
        if (!TypeKind.JAVA.equals(addedType.getKind())) {
            return;
        }
        for (final Type type : getAllTypes()) {
            if (!TypeKind.JAVA.equals(type.getKind())) {
                continue;
            }

            boolean changed = false;
            for (final Type.TypeField field : type.getFields()) {
                if (field.getQueryDataType() == null && !field.getQueryDataTypeMetadata().isEmpty()) {
                    if (addedType.getJavaClassName().equals(field.getQueryDataTypeMetadata())) {
                        field.setQueryDataType(addedType.toQueryDataTypeRef());
                        changed = true;
                    }
                }
            }
            if (changed) {
                put(type.getName(), type);
            }
        }
    }

    private List<Type.TypeField> getFieldsFromJavaClass(final Class<?> typeClass, final Type thisType) {
        return FieldsUtil.resolveClass(typeClass).entrySet().stream()
                .map(entry -> {
                    final QueryDataType queryDataType;
                    if (isJavaClass(entry.getValue())) {
                        if (entry.getValue().getName().equals(thisType.getJavaClassName())) {
                            queryDataType = thisType.toQueryDataTypeRef();
                        } else {
                            final Type existingType = getTypeByClass(entry.getValue());
                            if (existingType != null) {
                                queryDataType = existingType.toQueryDataTypeRef();
                            } else {
                                queryDataType = null;
                            }
                        }

                        if (queryDataType == null) {
                            return new Type.TypeField(entry.getKey(), entry.getValue().getName());
                        }
                    } else {
                        queryDataType = QueryDataTypeUtils.resolveTypeForClass(entry.getValue());
                    }

                    return new Type.TypeField(entry.getKey(), queryDataType);
                })
                .collect(Collectors.toList());
    }

    private List<Type.TypeField> getFieldsFromGenericRecord(final Class<?> typeClass) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private boolean isJavaClass(Class<?> clazz) {
        return !clazz.isPrimitive() && !clazz.getPackage().getName().startsWith("java.");
    }
}
