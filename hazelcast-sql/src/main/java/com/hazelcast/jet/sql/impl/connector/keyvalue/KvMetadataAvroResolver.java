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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.reduce;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getMetadata;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;
import static com.hazelcast.jet.sql.impl.inject.AvroUpsertTarget.CONVERSION_PREFS;
import static com.hazelcast.sql.impl.type.converter.Converters.getConverter;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

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

        public static final Map<Schema.Type, QueryDataType> AVRO_TO_SQL = new EnumMap<>(Map.of(
                Schema.Type.BOOLEAN, QueryDataType.BOOLEAN,
                Schema.Type.INT, QueryDataType.INT,
                Schema.Type.LONG, QueryDataType.BIGINT,
                Schema.Type.FLOAT, QueryDataType.REAL,
                Schema.Type.DOUBLE, QueryDataType.DOUBLE,
                Schema.Type.STRING, QueryDataType.VARCHAR,
                Schema.Type.UNION, QueryDataType.OBJECT,
                Schema.Type.NULL, QueryDataType.OBJECT
        ));

        private static final Map<QueryDataTypeFamily, List<Schema.Type>> CONVERSIONS =
                CONVERSION_PREFS.entrySet().stream().collect(
                        toMap(e -> getConverter(e.getKey()).getTypeFamily(), Entry::getValue));
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
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);
        for (QueryPath path : fieldsByPath.keySet()) {
            if (path.isTopLevel()) {
                throw QueryException.error("Cannot use the '" + path + "' field with Avro serialization");
            }
        }

        Schema schema = getSchema(fieldsByPath, options, isKey);
        if (schema != null && options.containsKey("schema.registry.url")) {
            throw new IllegalArgumentException("Inline schema cannot be used with schema registry");
        }

        if (userFields.isEmpty()) {
            if (schema == null) {
                throw QueryException.error(
                        "Either a column list or an inline schema is required to create Avro-based mapping");
            }
            return resolveFields(schema, (name, type) ->
                    new MappingField(name, type, new QueryPath(name, isKey).toString()));
        } else {
            if (schema != null) {
                validate(schema, getFields(fieldsByPath).collect(toList()));
            }
            return fieldsByPath.values().stream();
        }
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

        Schema schema = getSchema(fieldsByPath, options, isKey);
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
        return reduce(SchemaBuilder.record(recordName).fields(), fields, (schema, field) -> {
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
                            ? resolveSchema(field.type().getObjectTypeName(),
                                    field.type().getObjectFields().stream().map(Field::new))
                            : Schemas.OBJECT_SCHEMA;
                    return schema.name(field.name()).type(optional(fieldSchema)).withDefault(null);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + field.type());
            }
        }).endRecord();
    }

    public static <T> Stream<T> resolveFields(Schema schema, BiFunction<String, QueryDataType, T> constructor) {
        return schema.getFields().stream().map(field -> {
            Schema fieldSchema = unwrapNullableType(field.schema());
            if (fieldSchema.getType() == RECORD) {
                throw QueryException.error("Column list is required to create nested fields");
            }
            QueryDataType type = Schemas.AVRO_TO_SQL.get(fieldSchema.getType());
            if (type == null) {
                throw new IllegalArgumentException("Unsupported schema type: " + fieldSchema.getType());
            }
            return constructor.apply(field.name(), type);
        });
    }

    private static void validate(Schema schema, List<Field> fields) {
        if (schema.getType() != RECORD) {
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

            List<Schema.Type> conversions = mappingFieldTypeFamily == QueryDataTypeFamily.OBJECT
                    ? mappingFieldType.isCustomType()
                            ? List.of(RECORD)  // Unwrapped, so does not include NULL
                            : List.of(UNION, NULL)  // Ordinary OBJECT can be mapped to NULL
                    : Schemas.CONVERSIONS.get(mappingFieldTypeFamily);
            if (conversions == null) {
                throw new IllegalArgumentException("Unsupported type: " + mappingFieldType);
            }

            Schema.Field schemaField = schema.getField(path);
            if (schemaField == null) {
                throw new IllegalArgumentException("Field '" + path + "' does not exist in schema");
            }

            Schema fieldSchema = unwrapNullableType(schemaField.schema());
            Schema.Type schemaFieldType = fieldSchema.getType();
            if (!conversions.contains(schemaFieldType)) {
                throw new IllegalArgumentException(schemaFieldType + " schema type is incompatible with "
                        + mappingFieldType + " mapping type");
            }

            if (mappingFieldType.isCustomType()) {
                validate(fieldSchema, mappingFieldType.getObjectFields().stream().map(Field::new).collect(toList()));
            }
        }
    }

    private static Schema getSchema(Map<QueryPath, MappingField> fields, Map<String, String> options, boolean isKey) {
        return getMetadata(fields)
                .map(json -> new Schema.Parser().parse(json))
                .orElseGet(() -> inlineSchema(options, isKey));
    }

    /**
     * A field is <em>nullable</em> if it can be set to null. A nullable field is
     * <em>optional</em> if it can have null default value, which is only possible if
     * its type is {@code NULL} or a {@code UNION} with {@code NULL} as the first element.
     */
    public static Schema optional(Schema schema) {
        if (schema.getType() == UNION) {
            return schema.getTypes().get(0).getType() == NULL ? schema
                    : reduce(
                            SchemaBuilder.unionOf().nullType(),
                            schema.getTypes().stream().filter(type -> type.getType() != NULL),
                            (union, type) -> union.and().type(type)
                    ).endUnion();
        }
        return schema.getType() == NULL ? schema
                : SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
    }

    public static Schema inlineSchema(Map<String, String> options, boolean isKey) {
        String json = options.get(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA);
        return json != null ? new Schema.Parser().parse(json) : null;
    }
}
