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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;

/**
 * Interface for key-value resolution of fields for a particular
 * serialization types.
 */
public interface KvMetadataResolver {

    Stream<String> supportedFormats();

    Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    );

    KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    );

    static Map<QueryPath, MappingField> extractFields(List<MappingField> fields, boolean isKey) {
        Map<QueryPath, MappingField> fieldsByPath = new LinkedHashMap<>();
        for (MappingField field : fields) {
            QueryPath path = QueryPath.create(field.externalName());
            if (isKey != path.isKey()) {
                continue;
            }
            if (fieldsByPath.putIfAbsent(path, field) != null) {
                throw QueryException.error("Duplicate external name: " + path);
            }
        }
        return fieldsByPath;
    }

    static void maybeAddDefaultField(
            boolean isKey,
            @Nonnull List<MappingField> resolvedFields,
            @Nonnull List<TableField> tableFields,
            @Nonnull QueryDataType type
    ) {
        // Add the default `__key` or `this` field as hidden, if not present in the field names
        String fieldName = isKey ? KEY : VALUE;
        if (resolvedFields.stream().noneMatch(field -> field.name().equals(fieldName))) {
            tableFields.add(new MapTableField(fieldName, type, true, QueryPath.create(fieldName)));
        }
    }
}
