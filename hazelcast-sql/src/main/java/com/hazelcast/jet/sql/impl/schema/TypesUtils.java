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

import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashMap;
import java.util.Map;

public final class TypesUtils {
    private TypesUtils() { }

    public static QueryDataType convertTypeToQueryDataType(final Type rootType, final TypesStorage typesStorage) {
        final Map<String, QueryDataType> discoveredTypes = new HashMap<>();

        traverseTypeHierarchy(rootType, discoveredTypes, typesStorage);
        populateTypeInformation(discoveredTypes, typesStorage);

        return discoveredTypes.get(rootType.getName());
    }

    public static void traverseTypeHierarchy(final Type current,
                                             final Map<String, QueryDataType> discovered,
                                             final TablesStorage tablesStorage
    ) {
        // TODO: proper conversion for all kinds
        discovered.putIfAbsent(current.getName(), new QueryDataType(current.getName(), current.getJavaClassName()));

        for (int i = 0; i < current.getFields().size(); i++) {
            final Type.TypeField field = current.getFields().get(i);
            if (field.getQueryDataType().isCustomType()) {
                final String fieldTypeName = field.getQueryDataType().getObjectTypeName();
                if (!discovered.containsKey(fieldTypeName)) {
                    traverseTypeHierarchy(tablesStorage.getType(fieldTypeName), discovered, tablesStorage);
                }
            }
        }
    }

    public static void populateTypeInformation(final Map<String, QueryDataType> types, final TablesStorage tablesStorage) {
        for (final QueryDataType queryDataType : types.values()) {
            final Type type = tablesStorage.getType(queryDataType.getObjectTypeName());
            queryDataType.setObjectTypeClassName(type.getJavaClassName());
            for (int i = 0; i < type.getFields().size(); i++) {
                final Type.TypeField field = type.getFields().get(i);
                if (field.getQueryDataType().isCustomType()) {
                    queryDataType.getObjectFields().add(new QueryDataType.QueryDataTypeField(
                            field.getName(),
                            types.get(field.getQueryDataType().getObjectTypeName())
                    ));
                } else {
                    queryDataType.getObjectFields().add(new QueryDataType.QueryDataTypeField(
                            field.getName(),
                            field.getQueryDataType()
                    ));
                }
            }
        }
    }
}
