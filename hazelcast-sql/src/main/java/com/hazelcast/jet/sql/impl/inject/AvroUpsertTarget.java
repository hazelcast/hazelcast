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

import com.hazelcast.internal.util.collection.DefaultedMap;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.AVRO_TO_SQL;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;

@NotThreadSafe
class AvroUpsertTarget implements UpsertTarget {
    // In integers and decimals, we try to preserve the precision.
    // In floating-point numbers, we try to preserve the scale since they can
    // underflow, i.e. become 0 due to being very small, when converted to an integer.
    private static final Map<Class<?>, List<Schema.Type>> CONVERSION_PREFS = new DefaultedMap<>(Map.of(
            Boolean.class, List.of(BOOLEAN, STRING),
            Byte.class, List.of(INT, LONG, FLOAT, DOUBLE, STRING),
            Short.class, List.of(INT, LONG, FLOAT, DOUBLE, STRING),
            Integer.class, List.of(INT, LONG, DOUBLE, STRING, FLOAT),
            Long.class, List.of(LONG, INT, STRING, DOUBLE, FLOAT),
            Float.class, List.of(FLOAT, DOUBLE, STRING, INT, LONG),
            Double.class, List.of(DOUBLE, FLOAT, STRING, LONG, INT),
            BigDecimal.class, List.of(STRING, LONG, DOUBLE, INT, FLOAT)
    ), List.of(STRING));

    private final Schema schema;

    private GenericRecordBuilder record;

    AvroUpsertTarget(Schema schema) {
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        Schema fieldSchema = schema.getField(path).schema();
        Schema.Type schemaFieldType = unwrapNullableType(fieldSchema).getType();
        switch (schemaFieldType) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return value -> {
                    try {
                        record.set(path, AVRO_TO_SQL.get(schemaFieldType).convert(value));
                    } catch (QueryException e) {
                        throw QueryException.error("Cannot convert " + value + " to " + schemaFieldType
                                + " (field=" + path + ")");
                    }
                };
            case UNION:
                return value -> {
                    if (value == null) {
                        record.set(path, null);
                        return;
                    } else {
                        for (Schema.Type target : CONVERSION_PREFS.get(value.getClass())) {
                            if (fieldSchema.getTypes().stream().anyMatch(schema -> schema.getType() == target)) {
                                try {
                                    record.set(path, AVRO_TO_SQL.get(target).convert(value));
                                    return;
                                } catch (QueryException ignored) { }
                            }
                        }
                    }
                    throw QueryException.error("Not in union " + fieldSchema + ": " + value + " ("
                            + value.getClass().getSimpleName() + ") (field=" + path + ")");
                };
            case NULL:
                return value -> {
                    if (value != null) {
                        throw QueryException.error("Cannot convert " + value + " to NULL (field=" + path + ")");
                    }
                    record.set(path, null);
                };
            default:
                throw QueryException.error("Schema type " + schemaFieldType + " is unsupported (field=" + path + ")");
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
}
