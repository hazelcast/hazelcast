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
import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;

public final class KvMetadataAvroResolver implements KvMetadataResolver {

    public static final KvMetadataAvroResolver INSTANCE = new KvMetadataAvroResolver();

    private KvMetadataAvroResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(AVRO_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        if (userFields.isEmpty()) {
            throw QueryException.error("Column list is required for Avro format");
        }

        return extractFields(userFields, isKey).entrySet().stream()
                .map(entry -> {
                    QueryPath path = entry.getKey();
                    if (path.getPath() == null) {
                        throw QueryException.error("Cannot use the '" + path + "' field with Avro serialization");
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

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        return new KvMetadata(
                fields,
                AvroQueryTargetDescriptor.INSTANCE,
                new AvroUpsertTargetDescriptor(schema(fields).toString())
        );
    }

    private Schema schema(List<TableField> fields) {
        QueryPath[] paths = paths(fields);
        QueryDataType[] types = types(fields);

        FieldAssembler<Schema> schema = SchemaBuilder.record("jet.sql").fields();
        for (int i = 0; i < fields.size(); i++) {
            String path = paths[i].getPath();
            if (path == null) {
                continue;
            }
            QueryDataType type = types[i];
            switch (type.getTypeFamily()) {
                case BOOLEAN:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().booleanType().endUnion()
                                   .nullDefault();
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().intType().endUnion()
                                   .nullDefault();
                    break;
                case BIGINT:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().longType().endUnion()
                                   .nullDefault();
                    break;
                case REAL:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().floatType().endUnion()
                                   .nullDefault();
                    break;
                case DOUBLE:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().doubleType().endUnion()
                                   .nullDefault();
                    break;
                case DECIMAL:
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case VARCHAR:
                    schema = schema.name(path).type()
                                   .unionOf().nullType().and().stringType().endUnion()
                                   .nullDefault();
                    break;
                case OBJECT:
                    schema = schema.name(path).type()
                                   .unionOf()
                                   .nullType()
                                   .and().booleanType()
                                   .and().intType()
                                   .and().longType()
                                   .and().floatType()
                                   .and().doubleType()
                                   .and().stringType()
                                   .endUnion()
                                   .nullDefault();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type.getTypeFamily());
            }
        }
        return schema.endRecord();
    }

    private QueryPath[] paths(List<TableField> fields) {
        return fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
    }

    private QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }
}
