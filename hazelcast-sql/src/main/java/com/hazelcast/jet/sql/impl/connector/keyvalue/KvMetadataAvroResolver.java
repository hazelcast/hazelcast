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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getSchemaId;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getTableFields;
import static com.hazelcast.jet.sql.impl.inject.AvroUpsertTarget.CONVERSION_PREFS;
import static com.hazelcast.sql.impl.type.converter.Converters.getConverter;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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

        private static final Map<QueryDataTypeFamily, List<Schema.Type>> CONVERSIONS =
                CONVERSION_PREFS.entrySet().stream()
                        .collect(ImmutableMap::<QueryDataTypeFamily, List<Schema.Type>>builder,
                                 (map, e) -> map.put(getConverter(e.getKey()).getTypeFamily(), e.getValue()),
                                 ExceptionUtil::notParallelizable)
                        .put(QueryDataTypeFamily.OBJECT, List.of(Schema.Type.RECORD, Schema.Type.UNION, Schema.Type.NULL))
                        .build();
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
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);
        fieldsByPath.forEach((path, field) -> {
            if (path.isTopLevel() && !field.type().isCustomType()) {
                throw QueryException.error("'" + path + "' field must be used with a user-defined type");
            }
        });

        Schema schema = getSchemaId(fieldsByPath, schemaJson -> {
            // HazelcastKafkaAvro[De]Serializer obtains the schema from mapping options
            options.put(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA, schemaJson);
            return new Schema.Parser().parse(schemaJson);
        }, () -> inlineSchema(options, isKey));

        if (schema != null) {
            if (options.containsKey("schema.registry.url")) {
                throw new IllegalArgumentException("Inline schema cannot be used with schema registry");
            }
            validate(schema, getFields(fieldsByPath).collect(toList()));
        }
        return fieldsByPath.values().stream();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(resolvedFields, isKey);
        List<TableField> fields = getTableFields(isKey, fieldsByPath, QueryDataType.OBJECT);

        Schema schema = getSchemaId(fieldsByPath, json -> new Schema.Parser().parse(json),
                () -> inlineSchema(options, isKey));
        if (schema == null) {
            String recordName = options.getOrDefault(
                    isKey ? OPTION_KEY_AVRO_RECORD_NAME : OPTION_VALUE_AVRO_RECORD_NAME, "jet.sql");
            schema = resolveSchema(recordName, getFields(fieldsByPath));
        }
        return new KvMetadata(
                fields,
                AvroQueryTargetDescriptor.INSTANCE,
                new AvroUpsertTargetDescriptor(schema)
        );
    }

    // CREATE MAPPING <name> (<fields>) Type Kafka; INSERT INTO <name> ...
    private static Schema resolveSchema(String recordName, Stream<Field> fields) {
        return fields.reduce(SchemaBuilder.record(recordName).fields(), (schema, field) -> {
            switch (field.type().getTypeFamily()) {
                case BOOLEAN:
                    return schema.optionalBoolean(field.name());
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    return schema.optionalInt(field.name());
                case BIGINT:
                    return schema.optionalLong(field.name());
                case REAL:
                    return schema.optionalFloat(field.name());
                case DOUBLE:
                    return schema.optionalDouble(field.name());
                case DECIMAL:
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case VARCHAR:
                    return schema.optionalString(field.name());
                case OBJECT:
                    Schema fieldSchema = field.type().isCustomType()
                            ? resolveSchema(field.type().getObjectTypeName(), getFields(field.type()))
                            : Schemas.OBJECT_SCHEMA;
                    return optionalField(field.name(), fieldSchema).apply(schema);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + field.type());
            }
        }, ExceptionUtil::notParallelizable).endRecord();
    }

    private static void validate(Schema schema, List<Field> fields) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be an Avro record");
        }
        Set<String> mappingFields = fields.stream().map(Field::name).collect(toSet());
        for (Schema.Field schemaField : schema.getFields()) {
            if (!schemaField.hasDefaultValue() && !mappingFields.contains(schemaField.name())) {
                throw new IllegalArgumentException("Mandatory field '" + schemaField.name()
                        + "' is not mapped to any column");
            }
        }
        for (Field field : fields) {
            String path = field.name();
            QueryDataType mappingFieldType = field.type();
            QueryDataTypeFamily mappingFieldTypeFamily = mappingFieldType.getTypeFamily();

            List<Schema.Type> conversions = Schemas.CONVERSIONS.get(mappingFieldTypeFamily);
            if (conversions == null) {
                throw new IllegalArgumentException("Unsupported type: " + mappingFieldType);
            }

            Schema.Field schemaField = schema.getField(path);
            if (schemaField == null) {
                throw new IllegalArgumentException("Field '" + path + "' does not exist in schema");
            }

            Schema fieldSchema = unwrapNullableType(schemaField.schema());
            Schema.Type schemaFieldType = fieldSchema.getType();
            if (!(mappingFieldTypeFamily == QueryDataTypeFamily.OBJECT
                    ? mappingFieldType.isCustomType() ? conversions.subList(0, 1) : conversions.subList(1, 3)
                    : conversions).contains(schemaFieldType)) {
                throw new IllegalArgumentException(schemaFieldType + " schema type is incompatible with "
                        + mappingFieldType + " mapping type");
            }

            if (mappingFieldType.isCustomType()) {
                validate(fieldSchema, getFields(mappingFieldType).collect(toList()));
            }
        }
    }

    public static UnaryOperator<FieldAssembler<Schema>> optionalField(String name, Schema schema) {
        return schema.isNullable()
                ? builder -> builder.name(name).type(schema).withDefault(null)
                : builder -> builder.name(name).type().optional().type(schema);
    }

    public static Schema inlineSchema(Map<String, String> options, Boolean isKey) {
        String schemaProperty = isKey == null ? OPTION_TYPE_AVRO_SCHEMA :
                isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA;
        String schemaJson = options.get(schemaProperty);
        if (schemaJson == null) {
            if (isKey != null && !options.containsKey("schema.registry.url")) {
                throw QueryException.error("Either schema.registry.url or " + schemaProperty
                        + " is required to create Avro-based mapping");
            }
            return null;
        }
        return new Schema.Parser().parse(schemaJson);
    }
}
