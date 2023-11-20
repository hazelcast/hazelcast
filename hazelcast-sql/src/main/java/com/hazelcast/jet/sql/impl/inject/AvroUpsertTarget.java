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
import com.hazelcast.internal.util.collection.DefaultedMap;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.Sets.toImmutableEnumSet;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.Schemas.AVRO_TO_SQL;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;

@NotThreadSafe
public class AvroUpsertTarget implements UpsertTarget {
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

    private GenericRecordBuilder record;

    AvroUpsertTarget(Schema schema) {
        this.schema = schema;
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }
        Injector injector = createInjector(schema, path, type);
        return value -> injector.set(record, value);
    }

    private Injector createInjector(Schema schema, String path, QueryDataType type) {
        Schema fieldSchema = unwrapNullableType(schema.getField(path).schema());
        Schema.Type fieldSchemaType = fieldSchema.getType();
        switch (fieldSchemaType) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                QueryDataType targetType = AVRO_TO_SQL.get(fieldSchemaType);
                return (record, value) -> {
                    try {
                        record.set(path, targetType.convert(value));
                    } catch (QueryException e) {
                        throw QueryException.error("Cannot convert " + value + " to " + fieldSchemaType
                                + " (field=" + path + ")");
                    }
                };
            case RECORD:
                List<Injector> injectors = type.getObjectFields().stream()
                        .map(field -> createInjector(fieldSchema, field.getName(), field.getType()))
                        .collect(toList());
                return (record, value) -> {
                    if (value == null) {
                        record.set(path, null);
                        return;
                    }
                    GenericRecordBuilder nestedRecord = new GenericRecordBuilder(fieldSchema);
                    for (int i = 0; i < injectors.size(); i++) {
                        injectors.get(i).set(nestedRecord, ((RowValue) value).getValues().get(i));
                    }
                    record.set(path, nestedRecord.build());
                };
            case UNION:
                Predicate<Schema.Type> hasType = fieldSchema.getTypes().stream()
                        .map(Schema::getType).collect(toImmutableEnumSet())::contains;
                DefaultedMap<Class<?>, List<QueryDataType>> availableTargets =
                        CONVERSION_PREFS.mapKeysAndValues(identity(), targets -> targets.stream()
                                .filter(hasType).map(AVRO_TO_SQL::get).collect(toList()));
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

    @Override
    public void init() {
        record = new GenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        Record record = this.record.build();
        this.record = null;
        return record;
    }

    @FunctionalInterface
    private interface Injector {
        void set(GenericRecordBuilder record, Object value);
    }
}
