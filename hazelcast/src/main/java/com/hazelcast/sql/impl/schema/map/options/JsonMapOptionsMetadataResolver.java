/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map.options;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.JsonUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;

// TODO: deduplicate with MapSampleMetadataResolver
public final class JsonMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    public static final JsonMapOptionsMetadataResolver INSTANCE = new JsonMapOptionsMetadataResolver();

    private JsonMapOptionsMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return JSON_SERIALIZATION_FORMAT;
    }

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> new QueryPath(name, false));

        if (externalFieldsByPath.isEmpty()) {
            throw QueryException.error("Empty " + (isKey ? "key" : "value") + " column list");
        }

        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();
        Set<String> pathsRequiringConversion = new HashSet<>();

        for (Entry<QueryPath, ExternalField> externalField : externalFieldsByPath.entrySet()) {
            QueryPath path = externalField.getKey();
            if (path.getPath() == null) {
                throw QueryException.error("Invalid external name '" + path.getFullPath() + "'");
            }
            QueryDataType type = externalField.getValue().type();
            String name = externalField.getValue().name();
            boolean requiresConversion = doesRequireConversion(type);

            MapTableField field = new MapTableField(name, type, false, path, requiresConversion);

            if (fields.putIfAbsent(field.getName(), field) == null && field.isRequiringConversion()) {
                pathsRequiringConversion.add(field.getPath().getPath());
            }
        }

        return new MapOptionsMetadata(
                new GenericQueryTargetDescriptor(pathsRequiringConversion),
                JsonUpsertTargetDescriptor.INSTANCE,
                fields
        );
    }

    private static boolean doesRequireConversion(QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
            // assuming values are homomorphic
            case BIGINT:
            case VARCHAR:
                return !type.isStatic();
            default:
                return true;
        }
    }
}
