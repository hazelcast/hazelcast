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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaWriter;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;

final class MetadataCompactResolver implements KvMetadataResolver {

    static final MetadataCompactResolver INSTANCE = new MetadataCompactResolver();

    private MetadataCompactResolver() {
    }

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

        String typeNameProperty = isKey ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME;
        String typeName = options.get(typeNameProperty);

        if (typeName == null) {
            throw QueryException.error("Unable to resolve table metadata. Missing '" + typeNameProperty + "' option");
        }

        Map<QueryPath, MappingField> fields = extractFields(userFields, isKey);
        return fields.entrySet().stream()
                .map(entry -> {
                    QueryPath path = entry.getKey();
                    if (path.getPath() == null) {
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

        String typeNameProperty = isKey ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME;
        String typeName = options.get(typeNameProperty);

        List<TableField> fields = new ArrayList<>(fieldsByPath.size());
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        Schema schema = resolveSchema(typeName, fieldsByPath);

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new CompactUpsertTargetDescriptor(schema)
        );
    }

    private Schema resolveSchema(String typeName, Map<QueryPath, MappingField> fields) {
        SchemaWriter schemaWriter = new SchemaWriter(typeName);
        for (Entry<QueryPath, MappingField> entry : fields.entrySet()) {
            String name = entry.getKey().getPath();
            QueryDataType type = entry.getValue().type();
            schemaWriter.addField(new FieldDescriptor(name, resolveToCompactKind(type.getTypeFamily())));
        }
        return schemaWriter.build();
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static FieldKind resolveToCompactKind(QueryDataTypeFamily type) {
        switch (type) {
            case BOOLEAN:
                return FieldKind.NULLABLE_BOOLEAN;
            case TINYINT:
                return FieldKind.NULLABLE_INT8;
            case SMALLINT:
                return FieldKind.NULLABLE_INT16;
            case INTEGER:
                return FieldKind.NULLABLE_INT32;
            case BIGINT:
                return FieldKind.NULLABLE_INT64;
            case REAL:
                return FieldKind.NULLABLE_FLOAT32;
            case DOUBLE:
                return FieldKind.NULLABLE_FLOAT64;
            case DECIMAL:
                return FieldKind.DECIMAL;
            case VARCHAR:
                return FieldKind.STRING;
            case TIME:
                return FieldKind.TIME;
            case DATE:
                return FieldKind.DATE;
            case TIMESTAMP:
                return FieldKind.TIMESTAMP;
            case TIMESTAMP_WITH_TIME_ZONE:
                return FieldKind.TIMESTAMP_WITH_TIMEZONE;
            default:
                throw new IllegalArgumentException("Compact format does not allow " + type + " data type");
        }
    }
}
