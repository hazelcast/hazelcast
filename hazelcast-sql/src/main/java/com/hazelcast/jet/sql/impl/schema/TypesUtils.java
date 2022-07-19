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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public final class TypesUtils {
    private TypesUtils() { }

    public static QueryDataType convertTypeToQueryDataType(final Type rootType, final TypesStorage typesStorage) {
        return convertTypeToQueryDataTypeInt(rootType.getName(), rootType, typesStorage, new HashMap<>());
    }

    /**
     * If `type` is null, `typeName` will be used to look it up from the storage.
     */
    private static QueryDataType convertTypeToQueryDataTypeInt(
            @Nonnull final String typeName,
            @Nullable Type type,
            @Nonnull final TablesStorage tablesStorage,
            @Nonnull final Map<String, QueryDataType> seen
    ) {
        QueryDataType convertedType = seen.get(typeName);
        if (convertedType != null) {
            return convertedType;
        }

        if (type == null) {
            type = tablesStorage.getType(typeName);
        }

        // At this point the `convertedType` lacks fields. We put it to the `seen` map for the purpose of resolving
        // cyclic references, we'll add the fields later below.
        convertedType = type.toQueryDataTypeRef();
        seen.putIfAbsent(type.getName(), convertedType);

        for (Type.TypeField field : type.getFields()) {
            QueryDataType queryDataType;
            if (field.getQueryDataType().isCustomType()) {
                queryDataType = convertTypeToQueryDataTypeInt(field.getQueryDataType().getObjectTypeName(),
                        null, tablesStorage, seen);
            } else {
                queryDataType = field.getQueryDataType();
            }

            convertedType.getObjectFields().add(
                    new QueryDataType.QueryDataTypeField(field.getName(), queryDataType));
        }

        return convertedType;
    }
}
