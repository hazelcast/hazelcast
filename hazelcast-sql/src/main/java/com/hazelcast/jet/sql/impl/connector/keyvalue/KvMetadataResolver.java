/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataType.QueryDataTypeField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;

/**
 * Interface for key-value resolution of fields for a particular
 * serialization type.
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

    /**
     * Converts {@link MappingField}s into {@link MapTableField}s, and adds the default
     * {@code __key}/{@code this} field as hidden if it is not present in the fields.
     */
    static List<TableField> getTableFields(
            boolean isKey,
            Map<QueryPath, MappingField> fields,
            QueryDataType defaultFieldType
    ) {
        List<TableField> tableFields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : fields.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            tableFields.add(new MapTableField(name, type, false, path));
        }

        // Add the default `__key` or `this` field as hidden, if not present in the field names
        String name = isKey ? KEY : VALUE;
        if (fields.values().stream().noneMatch(field -> field.name().equals(name))) {
            tableFields.add(new MapTableField(name, defaultFieldType, true, QueryPath.create(name)));
        }
        return tableFields;
    }

    /**
     * If {@code __key}/{@code this} is the only key/value field and has a custom type with
     * nonnull metadata, resolve the schema ID from the type. Otherwise, return {@code orElse()}.
     */
    static <T> T getSchemaId(
            Map<QueryPath, MappingField> fields,
            Function<String, T> resolveFromType,
            Supplier<T> orElse
    ) {
        return flatMap(fields, type -> {
            String metadata = type.getObjectTypeMetadata();
            return metadata != null ? resolveFromType.apply(metadata) : orElse.get();
        }, orElse);
    }

    /**
     * If {@code __key}/{@code this} is the only field and has a custom type,
     * return type fields. Otherwise, return mapping fields without {@code __key} or {@code this}.
     */
    static Stream<Field> getFields(Map<QueryPath, MappingField> fields) {
        return flatMap(fields, KvMetadataResolver::getFields, () -> fields.entrySet().stream()
                .filter(e -> !e.getKey().isTopLevel()).map(Field::new));
    }

    static Stream<Field> getFields(QueryDataType type) {
        return type.getObjectFields().stream().map(Field::new);
    }

    /**
     * If {@code __key}/{@code this} is the only field and has a custom type,
     * return {@code typeMapper(fieldType)}. Otherwise, return {@code orElse()}.
     */
    static <T> T flatMap(
            Map<QueryPath, MappingField> fields,
            Function<QueryDataType, T> typeMapper,
            Supplier<T> orElse
    ) {
        if (fields.size() == 1) {
            Entry<QueryPath, MappingField> entry = fields.entrySet().iterator().next();
            if (entry.getKey().isTopLevel() && entry.getValue().type().isCustomType()) {
                return typeMapper.apply(entry.getValue().type());
            }
        }
        return orElse.get();
    }

    class Field {
        private final String name;
        private final QueryDataType type;

        public Field(Entry<QueryPath, MappingField> entry) {
            name = entry.getKey().getPath();
            type = entry.getValue().type();
        }

        public Field(TableField field) {
            name = field instanceof MapTableField
                    ? ((MapTableField) field).getPath().getPath() : field.getName();
            type = field.getType();
        }

        public Field(QueryDataTypeField field) {
            name = field.getName();
            type = field.getDataType();
        }

        public String name() {
            return name;
        }

        public QueryDataType type() {
            return type;
        }
    }
}
