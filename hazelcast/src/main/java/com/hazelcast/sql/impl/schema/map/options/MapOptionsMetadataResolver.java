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

import static java.lang.String.format;

// TODO: deduplicate with MapSampleMetadataResolver
public interface MapOptionsMetadataResolver {

    String supportedFormat();

    MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    );

    default Map<QueryPath, ExternalField> extractFields(
            List<ExternalField> externalFields,
            boolean key,
            Function<String, QueryPath> defaultPathSupplier
    ) {
        Map<QueryPath, ExternalField> fieldsByPath = new LinkedHashMap<>();
        for (ExternalField externalField : externalFields) {
            String externalName = externalField.externalName();

            QueryPath path;
            if (externalName == null) {
                path = defaultPathSupplier.apply(externalField.name());
            } else if (QueryPath.KEY.equals(externalName)) {
                path = QueryPath.KEY_PATH;
            } else if (QueryPath.VALUE.equals(externalName)) {
                path = QueryPath.VALUE_PATH;
            } else if (externalName.startsWith(QueryPath.KEY_PREFIX) || externalName.startsWith(QueryPath.VALUE_PREFIX)) {
                // TODO: should be supported? move the validation to SqlTableColumn ?
                if (externalName.chars().filter(ch -> ch == '.').count() > 1) {
                    throw QueryException.error(
                            format("Invalid field external name - '%s'. Nested fields are not supported.", externalName)
                    );
                }
                path = QueryPath.create(externalName);
            } else {
                throw QueryException.error(
                        format("External name should start with either '%s' or '%s'", QueryPath.KEY, QueryPath.VALUE)
                );
            }

            if (path.isKey() == key) {
                ExternalField existingExternalField = fieldsByPath.putIfAbsent(path, externalField);
                if (existingExternalField != null) {
                    throw QueryException.error(
                            format("Ambiguous mapping for fields - '%s', '%s'",
                                    existingExternalField.name(), externalField.name())
                    );
                }
            }
        }
        return fieldsByPath;
    }
}
