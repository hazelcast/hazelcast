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
import com.hazelcast.sql.impl.extract.JsonQueryTargetDescriptor;
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
import static java.lang.String.format;
import static java.util.Arrays.asList;

// TODO: deduplicate with MapSampleMetadataResolver
public final class JsonMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    public static final JsonMapOptionsMetadataResolver INSTANCE = new JsonMapOptionsMetadataResolver();

    private static final Set<Class<?>> STATIC_TYPES = new HashSet<>(asList(
            QueryDataType.BOOLEAN.getConverter().getValueClass(),
            QueryDataType.BIGINT.getConverter().getValueClass(),
            QueryDataType.VARCHAR.getConverter().getValueClass()
    ));

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
        Map<QueryPath, ExternalField> externalFieldsByPath =
                extractFields(externalFields, isKey, name -> new QueryPath(name, false));

        if (externalFieldsByPath.isEmpty()) {
            throw QueryException.error(format("Empty %s column list", isKey ? "key" : "value"));
        }

        Set<String> staticallyTypedFieldPaths = new HashSet<>();
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        for (Entry<QueryPath, ExternalField> externalField : externalFieldsByPath.entrySet()) {
            QueryPath path = externalField.getKey();
            QueryDataType type = externalField.getValue().type();
            String name = externalField.getValue().name();
            boolean staticallyTyped = STATIC_TYPES.contains(type.getConverter().getValueClass());

            MapTableField field = new MapTableField(name, type, false, path, staticallyTyped);

            if (field.isStaticallyTyped()) {
                staticallyTypedFieldPaths.add(field.getPath().getPath());
            }
            fields.put(field.getName(), field);
        }

        return new MapOptionsMetadata(
                new JsonQueryTargetDescriptor(staticallyTypedFieldPaths),
                JsonUpsertTargetDescriptor.INSTANCE,
                fields
        );
    }
}
