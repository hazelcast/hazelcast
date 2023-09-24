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

package com.hazelcast.jet.sql.impl.inject;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.DefaultedMap;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.toImmutableEnumSet;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.AVRO_TO_SQL;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;

@NotThreadSafe
public class AvroUpsertTarget extends UpsertTarget {
    // We try to preserve the precision. Floating-point numbers may underflow,
    // i.e. become 0 due to being very small, when converted to an integer.
    public static final DefaultedMap<Class<?>, List<Schema.Type>> CONVERSION_PREFS = new DefaultedMap<>(
            // TODO We may consider supporting temporal types <-> int/long conversions (globally).
            ImmutableMap.<Class<?>, List<Schema.Type>>builder()
                    .put(Boolean.class, List.of(BOOLEAN, STRING))
                    .put(Byte.class, List.of(INT, LONG, FLOAT, DOUBLE, STRING))
                    .put(Short.class, List.of(INT, LONG, FLOAT, DOUBLE, STRING))
                    .put(Integer.class, List.of(INT, LONG, DOUBLE, STRING, FLOAT))
                    .put(Long.class, List.of(LONG, INT, STRING, DOUBLE, FLOAT))
                    .put(Float.class, List.of(FLOAT, DOUBLE, STRING, INT, LONG))
                    .put(Double.class, List.of(DOUBLE, FLOAT, STRING, LONG, INT))
                    .put(BigDecimal.class, List.of(STRING, LONG, DOUBLE, INT, FLOAT))
                    .put(String.class, List.of(STRING, LONG, DOUBLE, INT, FLOAT, BOOLEAN))
                    .put(LocalTime.class, List.of(STRING))
                    .put(LocalDate.class, List.of(STRING))
                    .put(LocalDateTime.class, List.of(STRING))
                    .put(OffsetDateTime.class, List.of(STRING))
                    .build(),
            List.of(STRING));

    private final Schema schema;

    AvroUpsertTarget(Schema schema, InternalSerializationService serializationService) {
        super(serializationService);
        this.schema = schema;
    }

    @Override
    protected Converter<GenericRecord> createConverter(Stream<Field> fields) {
        return createConverter(schema, fields);
    }

    private Converter<GenericRecord> createConverter(Schema schema, Stream<Field> fields) {
        Injector<GenericRecordBuilder> injector = createRecordInjector(fields,
                field -> createInjector(schema, field.name(), field.type()));
        return value -> {
            if (value == null || (value instanceof GenericRecord
                    && ((GenericRecord) value).getSchema().equals(schema))) {
                return (GenericRecord) value;
            }
            GenericRecordBuilder record = new GenericRecordBuilder(schema);
            injector.set(record, value);
            return record.build();
        };
    }

    private Injector<GenericRecordBuilder> createInjector(Schema schema, String path, QueryDataType type) {
        Schema fieldSchema = unwrapNullableType(schema.getField(path).schema());
        Schema.Type fieldSchemaType = fieldSchema.getType();
        switch (fieldSchemaType) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                QueryDataType targetType = AVRO_TO_SQL.getOrDefault(fieldSchemaType);
                return (record, value) -> {
                    try {
                        record.set(path, targetType.convert(value));
                    } catch (QueryException e) {
                        throw QueryException.error("Cannot convert " + value + " to " + fieldSchemaType
                                + " (field=" + path + ")");
                    }
                };
            case RECORD:
                Converter<GenericRecord> converter = createConverter(fieldSchema, getFields(type));
                return (record, value) -> record.set(path, converter.apply(value));
            case UNION:
                Predicate<Schema.Type> hasType = fieldSchema.getTypes().stream()
                        .map(Schema::getType).collect(toImmutableEnumSet())::contains;
                DefaultedMap<Class<?>, List<QueryDataType>> availableTargets =
                        CONVERSION_PREFS.mapKeysAndValues(identity(), targets -> targets.stream()
                                .filter(hasType).map(AVRO_TO_SQL::getOrDefault).collect(toList()));
                return (record, value) -> {
                    if (value == null) {
                        record.set(path, null);
                        return;
                    }
                    for (QueryDataType target : availableTargets.getOrDefault(value.getClass())) {
                        try {
                            record.set(path, target.convert(value));
                            return;
                        } catch (QueryException ignored) { }
                    }
                    throw QueryException.error("Not in union " + fieldSchema + ": " + value + " ("
                            + value.getClass().getSimpleName() + ") (field=" + path + ")");
                };
            case NULL:
                return (record, value) -> {
                    if (value != null) {
                        throw QueryException.error("Cannot convert " + value + " to NULL (field=" + path + ")");
                    }
                    record.set(path, null);
                };
            default:
                throw QueryException.error("Schema type " + fieldSchemaType + " is unsupported (field=" + path + ")");
        }
    }
}
