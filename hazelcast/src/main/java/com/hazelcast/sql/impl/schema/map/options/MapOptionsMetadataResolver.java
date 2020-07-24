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
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

// TODO: deduplicate with MapSampleMetadataResolver
public interface MapOptionsMetadataResolver {

    String supportedFormat();

    MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    );

    default Map<QueryPath, ExternalField> extractKeyFields(
            List<ExternalField> externalFields
    ) {
        Map<QueryPath, ExternalField> keyFieldsByPath = new LinkedHashMap<>();
        for (ExternalField externalField : externalFields) {
            String externalName = externalField.externalName();

            if (externalName == null
                    || QueryPath.VALUE.equals(externalName)
                    || externalName.startsWith(QueryPath.VALUE_PREFIX)) {
                continue;
            }

            QueryPath path;
            if (QueryPath.KEY.equals(externalName)) {
                path = QueryPath.KEY_PATH;
            } else if (externalName.startsWith(QueryPath.KEY_PREFIX)) {
                path = QueryPath.create(externalName);
            } else {
                throw QueryException.error("Invalid external name '" + externalName + "'");
            }

            if (keyFieldsByPath.putIfAbsent(path, externalField) != null) {
                throw QueryException.error("Duplicate key external name '" + path + "'");
            }
        }
        return keyFieldsByPath;
    }

    default Map<QueryPath, ExternalField> extractValueFields(
            List<ExternalField> externalFields,
            Function<String, QueryPath> defaultPathSupplier
    ) {
        Map<QueryPath, ExternalField> valueFieldsByPath = new LinkedHashMap<>();
        for (ExternalField externalField : externalFields) {
            String externalName = externalField.externalName();

            if (externalName != null
                    && (QueryPath.KEY.equals(externalName) || externalName.startsWith(QueryPath.KEY_PREFIX))) {
                continue;
            }

            QueryPath path;
            if (externalName == null) {
                path = defaultPathSupplier.apply(externalField.name());
            } else if (QueryPath.VALUE.equals(externalName)) {
                path = QueryPath.VALUE_PATH;
            } else if (externalName.startsWith(QueryPath.VALUE_PREFIX)) {
                path = QueryPath.create(externalName);
            } else {
                throw QueryException.error("Invalid external name '" + externalName + "'");
            }

            if (valueFieldsByPath.putIfAbsent(path, externalField) != null) {
                throw QueryException.error("Duplicate value external name '" + path + "'");
            }
        }
        return valueFieldsByPath;
    }
}
