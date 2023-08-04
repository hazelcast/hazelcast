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
import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;
import static java.util.Arrays.asList;

public final class KvMetadataAvroResolver implements KvMetadataResolver {

    public static final KvMetadataAvroResolver INSTANCE = new KvMetadataAvroResolver();

    /**
     * Avro is an optional dependency for SQL, so the schemas should be the initialized lazily.
     */
    public static class Schemas {
        /**
         * {@link QueryDataType#OBJECT} is mapped to the union of all primitive types and null.
         * Avro {@link Schema.Type#RECORD}s can only be represented using user-defined types.
         */
        public static final Schema OBJECT_SCHEMA = SchemaBuilder.unionOf()
                .nullType()
                .and().booleanType()
                .and().intType()
                .and().longType()
                .and().floatType()
                .and().doubleType()
                .and().stringType()
                .endUnion();
    }

    private KvMetadataAvroResolver() { }

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
        String inlineSchema = options.get(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA);
        if (inlineSchema != null && options.containsKey("schema.registry.url")) {
            throw new IllegalArgumentException("Inline schema cannot be used with schema registry");
        }
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);
        if (inlineSchema != null) {
            Schema schema = new Schema.Parser().parse(inlineSchema);
            validate(schema, fieldsByPath);
        }
        return fieldsByPath.entrySet().stream()
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

        Schema schema;
        String inlineSchema = options.get(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA);
        if (inlineSchema != null) {
            schema = new Schema.Parser().parse(inlineSchema);
        } else {
            String recordName = options.getOrDefault(
                    isKey ? OPTION_KEY_AVRO_RECORD_NAME : OPTION_VALUE_AVRO_RECORD_NAME, "jet.sql");
            schema = schema(recordName, fields);
        }
        return new KvMetadata(
                fields,
                AvroQueryTargetDescriptor.INSTANCE,
                new AvroUpsertTargetDescriptor(schema)
        );
    }

    // CREATE MAPPING <name> (<fields>) Type Kafka; INSERT INTO <name> ...
    private Schema schema(String recordName, List<TableField> fields) {
        FieldAssembler<Schema> schema = SchemaBuilder.record(recordName).fields();
        for (TableField field : fields) {
            String path = ((MapTableField) field).getPath().getPath();
            if (path == null) {
                continue;
            }
            switch (field.getType().getTypeFamily()) {
                case BOOLEAN:
                    schema = schema.optionalBoolean(path);
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    schema = schema.optionalInt(path);
                    break;
                case BIGINT:
                    schema = schema.optionalLong(path);
                    break;
                case REAL:
                    schema = schema.optionalFloat(path);
                    break;
                case DOUBLE:
                    schema = schema.optionalDouble(path);
                    break;
                case DECIMAL:
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case VARCHAR:
                    schema = schema.optionalString(path);
                    break;
                case OBJECT:
                    schema = schema.name(path).type(Schemas.OBJECT_SCHEMA).withDefault(null);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + field.getType().getTypeFamily());
            }
        }
        return schema.endRecord();
    }

    private void validate(Schema schema, Map<QueryPath, MappingField> fieldsByPath) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be an Avro record");
        }
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            String path = entry.getKey().getPath();
            if (path == null) {
                continue;
            }
            Schema.Field schemaField = schema.getField(path);
            if (schemaField == null) {
                throw new IllegalArgumentException("Field '" + path + "' does not exist in schema");
            }
            Schema.Type schemaFieldType = unwrapNullableType(schemaField.schema()).getType();
            QueryDataTypeFamily mappingFieldType = entry.getValue().type().getTypeFamily();
            VarargsConsumer<Schema.Type> assertCompatible = types -> {
                if (!asList(types).contains(schemaFieldType)) {
                    throw new IllegalArgumentException(schemaFieldType + " schema type is incompatible with "
                            + mappingFieldType + " mapping type");
                }
            };
            // Logical types are not supported, which require changes in IntegerConverter and LongConverter.
            switch (mappingFieldType) {
                case BOOLEAN:
                    assertCompatible.accept(Schema.Type.BOOLEAN, Schema.Type.STRING);
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case REAL:
                case DOUBLE:
                case DECIMAL:
                    assertCompatible.accept(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                            Schema.Type.STRING);
                    break;
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                    assertCompatible.accept(Schema.Type.STRING);
                    break;
                case VARCHAR:
                    assertCompatible.accept(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                            Schema.Type.BOOLEAN, Schema.Type.STRING);
                    break;
                case OBJECT:
                    assertCompatible.accept(Schema.Type.UNION, Schema.Type.NULL);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + mappingFieldType);
            }
        }
    }

    @FunctionalInterface
    interface VarargsConsumer<T> extends Consumer<T[]> {
        @Override
        @SuppressWarnings("unchecked")
        void accept(T... args);
    }
}
