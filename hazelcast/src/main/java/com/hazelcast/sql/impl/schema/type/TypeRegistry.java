/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.type;

import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

// Temporary workaround
public class TypeRegistry {
    public static final TypeRegistry INSTANCE = new TypeRegistry();

    // class name
    private final Map<String, Type> registry = new HashMap<>();

    public Type getTypeByName(final String name) {
        return registry.get(name.toLowerCase(Locale.ROOT));
    }

    public Type getTypeByClass(final Class<?> typeClass) {
        return registry.entrySet().stream()
                .filter(entry -> entry.getValue().getJavaClassName().equals(typeClass.getName()))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
    }

    public void registerType(final String name, final Class<?> typeClass) {
        if (getTypeByClass(typeClass) != null || getTypeByName(name) != null) {
            return;
        }

        final Type type = new Type();
        type.setName(name);
        type.setJavaClassName(typeClass.getName());
        type.setQueryDataType(new QueryDataType(name));

        if (typeClass.isAssignableFrom(GenericRecord.class)) {
            type.setFields(getFieldsFromGenericRecord(typeClass));
        } else {
            type.setFields(getFieldsFromJavaClass(typeClass, type));
        }

        registry.put(name.toLowerCase(Locale.ROOT), type);
        fixTypeReferences(type);
    }

    private void fixTypeReferences(final Type addedType) {
        for (final Map.Entry<String, Type> entry : registry.entrySet()) {
            final Type type = entry.getValue();

            for (final Type.TypeField field : type.getFields()) {
                if (field.getQueryDataType() == null && !field.getClassName().isEmpty()) {
                    if (addedType.getJavaClassName().equals(field.getClassName())) {
                        field.setQueryDataType(addedType.getQueryDataType());
                    }
                }
            }
        }
    }

    private List<Type.TypeField> getFieldsFromJavaClass(final Class<?> typeClass, final Type thisType) {
        return FieldsUtil.resolveClass(typeClass).entrySet().stream()
                .map(entry -> {
                    final QueryDataType queryDataType;
                    if (isJavaClass(entry.getValue())) {
                        if (entry.getValue().getName().equals(thisType.getJavaClassName())) {
                            queryDataType = thisType.getQueryDataType();
                        } else {
                            final Type existingType = getTypeByClass(entry.getValue());
                            if (existingType != null) {
                                queryDataType = existingType.getQueryDataType();
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
        return !clazz.getPackage().getName().startsWith("java.");
    }
}
