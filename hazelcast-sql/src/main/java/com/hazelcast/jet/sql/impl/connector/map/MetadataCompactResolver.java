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

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaWriter;
import com.hazelcast.internal.util.collection.DefaultedMap;
import com.hazelcast.internal.util.collection.DefaultedMap.DefaultedMapBuilder;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.CompactUpsertTargetDescriptor;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.collect;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getMetadata;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;

public final class MetadataCompactResolver implements KvMetadataResolver {
    static final MetadataCompactResolver INSTANCE = new MetadataCompactResolver();

    private static final DefaultedMap<QueryDataTypeFamily, FieldKind> SQL_TO_COMPACT = new DefaultedMapBuilder<>(
            new EnumMap<>(ImmutableMap.<QueryDataTypeFamily, FieldKind>builder()
                    .put(QueryDataTypeFamily.BOOLEAN, FieldKind.NULLABLE_BOOLEAN)
                    .put(QueryDataTypeFamily.TINYINT, FieldKind.NULLABLE_INT8)
                    .put(QueryDataTypeFamily.SMALLINT, FieldKind.NULLABLE_INT16)
                    .put(QueryDataTypeFamily.INTEGER, FieldKind.NULLABLE_INT32)
                    .put(QueryDataTypeFamily.BIGINT, FieldKind.NULLABLE_INT64)
                    .put(QueryDataTypeFamily.REAL, FieldKind.NULLABLE_FLOAT32)
                    .put(QueryDataTypeFamily.DOUBLE, FieldKind.NULLABLE_FLOAT64)
                    .put(QueryDataTypeFamily.DECIMAL, FieldKind.DECIMAL)
                    .put(QueryDataTypeFamily.VARCHAR, FieldKind.STRING)
                    .put(QueryDataTypeFamily.TIME, FieldKind.TIME)
                    .put(QueryDataTypeFamily.DATE, FieldKind.DATE)
                    .put(QueryDataTypeFamily.TIMESTAMP, FieldKind.TIMESTAMP)
                    .put(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE, FieldKind.TIMESTAMP_WITH_TIMEZONE)
                    .put(QueryDataTypeFamily.OBJECT, FieldKind.COMPACT)
                    .build()))
            .orElseThrow(type -> new IllegalArgumentException("Compact format does not allow " + type + " data type"));

    private MetadataCompactResolver() { }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(COMPACT_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        if (userFields.isEmpty()) {
            throw QueryException.error("Column list is required for Compact format");
        }
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);

        // Check if the compact type name is specified
        getCompactTypeName(fieldsByPath, options, isKey);

        return fieldsByPath.entrySet().stream()
                .map(entry -> {
                    QueryPath path = entry.getKey();
                    if (path.isTopLevel()) {
                        throw QueryException.error("Cannot use the '" + path + "' field with Compact serialization");
                    }
                    QueryDataType type = entry.getValue().type();
                    if (type == QueryDataType.OBJECT) {
                        throw QueryException.error("Cannot derive Compact type for '" + type.getTypeFamily() + "'");
                    }
                    return entry.getValue();
                });
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(resolvedFields, isKey);

        String typeName = getCompactTypeName(fieldsByPath, options, isKey);

        List<TableField> fields = new ArrayList<>(fieldsByPath.size());
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        Schema schema = resolveSchema(typeName, getFields(fieldsByPath));

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new CompactUpsertTargetDescriptor(schema)
        );
    }

    private static Schema resolveSchema(String typeName, Stream<Field> fields) {
        return collect(new SchemaWriter(typeName), fields, (schema, field) ->
                schema.addField(new FieldDescriptor(field.name(),
                        SQL_TO_COMPACT.getOrDefault(field.type().getTypeFamily())))
        ).build();
    }

    private static String getCompactTypeName(
            Map<QueryPath, MappingField> fields,
            Map<String, String> options,
            boolean isKey
    ) {
        return getMetadata(fields).orElseGet(() -> compactTypeName(options, isKey));
    }

    public static String compactTypeName(Map<String, String> options, boolean isKey) {
        String typeNameProperty = isKey ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME;
        String typeName = options.get(typeNameProperty);
        if (typeName == null) {
            throw QueryException.error(typeNameProperty + " is required to create Compact-based mapping");
        }
        return typeName;
    }
}
