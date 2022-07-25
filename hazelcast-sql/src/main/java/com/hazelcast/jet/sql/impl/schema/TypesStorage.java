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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeKind;

// TODO: Merge into TablesStorage
public class TypesStorage extends TablesStorage {

    public TypesStorage(final NodeEngine nodeEngine) {
        super(nodeEngine);
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

    // TODO: replace with manual 2-stage initialization?
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
}
