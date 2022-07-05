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
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

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
                .filter(type -> {
                    final ClassDefinition classDef = type.getPortableClassDef();
                    return classDef.getFactoryId() == factoryId
                            && classDef.getClassId() == classId
                            && classDef.getVersion() == versionId;
                })
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
            result = putIfAbsent(name.toLowerCase(Locale.ROOT), type);
        } else {
            put(name.toLowerCase(Locale.ROOT), type);
        }

        fixTypeReferences(type);

        return result;
    }

    public boolean registerType(String name, ClassDefinition classDef) {
        final Type type = new Type();
        type.setName(name);
        type.setKind(TypeKind.PORTABLE);
        type.setPortableClassDef(classDef);
        final List<Type.TypeField> fields = new ArrayList<>();
        for (int i = 0; i < classDef.getFieldCount(); i++) {
            final FieldDefinition portableField = classDef.getField(i);
            final Type.TypeField typeField = new Type.TypeField();
            typeField.setName(portableField.getName());

            final QueryDataType queryDataType;
            if (portableField.getType().equals(FieldType.PORTABLE)) {
                queryDataType = getTypeByPortableClass(
                        portableField.getFactoryId(),
                        portableField.getClassId(),
                        portableField.getVersion()
                ).toQueryDataTypeRef();
            } else {
                queryDataType = resolvePortableFieldType(portableField.getType());
            }

            typeField.setQueryDataType(queryDataType);
            fields.add(typeField);
        }
        type.setFields(fields);

        put(name, type);

        return false;
    }

    private void fixTypeReferences(final Type addedType) {
        // TODO: type system consistency.
        for (final Type type : getAllTypes()) {
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

    @SuppressWarnings("checkstyle:ReturnCount")
    private QueryDataType resolvePortableFieldType(FieldType fieldType) {
        switch (fieldType) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case BYTE:
                return QueryDataType.TINYINT;
            case SHORT:
                return QueryDataType.SMALLINT;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            case DECIMAL:
                return QueryDataType.DECIMAL;
            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;
            case UTF:
                return QueryDataType.VARCHAR;
            case TIME:
                return QueryDataType.TIME;
            case DATE:
                return QueryDataType.DATE;
            case TIMESTAMP:
                return QueryDataType.TIMESTAMP;
            case TIMESTAMP_WITH_TIMEZONE:
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
            case PORTABLE:
            default:
                return QueryDataType.OBJECT;
        }
    }
}
