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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

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
    private static final Map<Class<?>, List<Schema.Type>> CONVERSION_PREFS = Map.of(
            Byte.class, List.of(INT, LONG, FLOAT, DOUBLE),
            Short.class, List.of(INT, LONG, FLOAT, DOUBLE),
            Integer.class, List.of(INT, LONG, DOUBLE, FLOAT),
            Long.class, List.of(LONG, DOUBLE, INT, FLOAT),
            Float.class, List.of(FLOAT, DOUBLE, INT, LONG),
            Double.class, List.of(DOUBLE, FLOAT, LONG, INT),
            BigDecimal.class, List.of(DOUBLE, FLOAT, LONG, INT)
    );
    private static final Map<Schema.Type, Converter> CONVERTERS = Map.of(
            INT, new Converter(AvroUpsertTarget::canConvertToInt, Number::intValue),
            LONG, new Converter(AvroUpsertTarget::canConvertToLong, Number::longValue),
            FLOAT, new Converter(AvroUpsertTarget::canConvertToFloat, Number::floatValue),
            DOUBLE, new Converter(AvroUpsertTarget::canConvertToDouble, Number::doubleValue)
    );
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
        Function<Object, Object> cannotConvert = number -> {
            throw QueryException.error("Cannot convert " + number + " to " + schemaFieldType + " (field=" + path + ")");
        };
        switch (schemaFieldType) {
            case NULL:
                return value -> record.set(path, value == null ? null : cannotConvert.apply(value));
            case BOOLEAN:
                return value -> record.set(path, value == null || value instanceof Boolean
                        ? value : Boolean.parseBoolean((String) value));
            case INT:
                return value -> record.set(path, value == null ? null : value instanceof Number
                        ? canConvertToInt(value) ? ((Number) value).intValue() : cannotConvert.apply(value)
                        : Integer.parseInt((String) value));
            case LONG:
                return value -> record.set(path, value == null ? null : value instanceof Number
                        ? canConvertToLong(value) ? ((Number) value).longValue() : cannotConvert.apply(value)
                        : Long.parseLong((String) value));
            case FLOAT:
                return value -> record.set(path, value == null ? null : value instanceof Number
                        ? canConvertToFloat(value) ? ((Number) value).floatValue() : cannotConvert.apply(value)
                        : Float.parseFloat((String) value));
            case DOUBLE:
                return value -> record.set(path, value == null ? null : value instanceof Number
                        ? canConvertToDouble(value) ? ((Number) value).doubleValue() : cannotConvert.apply(value)
                        : Double.parseDouble((String) value));
            case STRING:
                return value -> record.set(path, value == null ? null : value.toString());
            case UNION:
                return value -> {
                    Function<Schema.Type, Boolean> hasType = schemaType ->
                            fieldSchema.getTypes().stream().anyMatch(schema -> schema.getType() == schemaType);
                    boolean[] set = {false};
                    Consumer<Object> setField = v -> {
                        record.set(path, v);
                        set[0] = true;
                    };

                    if (value == null) {
                        setField.accept(null);
                    } else if (value instanceof Boolean) {
                        if (hasType.apply(BOOLEAN)) {
                            setField.accept(value);
                        }
                    } else if (value instanceof Number) {
                        for (Schema.Type target : CONVERSION_PREFS.get(value.getClass())) {
                            Converter converter = CONVERTERS.get(target);
                            if (hasType.apply(target) && converter.canConvert(value)) {
                                setField.accept(converter.convert((Number) value));
                                break;
                            }
                        }
                    }

                    if (!set[0]) {
                        if (hasType.apply(STRING)) {
                            setField.accept(value.toString());
                        } else {
                            throw QueryException.error("Not in union " + fieldSchema + ": " + value + " ("
                                    + value.getClass().getSimpleName() + ") (field=" + path + ")");
                        }
                    }
                };
            default:
                throw QueryException.error("Schema type " + schemaFieldType + " is unsupported (field=" + path + ")");
        }
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private static boolean canConvertToInt(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer
                || (value instanceof Long && (long) value == (int) (long) value)
                || (value instanceof Float && (float) value == (int) (float) value)
                || (value instanceof Double && (double) value == (int) (double) value)
                || (value instanceof BigDecimal && ((BigDecimal) value)
                        .compareTo(BigDecimal.valueOf(((Number) value).intValue())) == 0);
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private static boolean canConvertToLong(Object value) {
        return value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long
                || (value instanceof Float && (float) value == (long) (float) value)
                || (value instanceof Double && (double) value == (long) (double) value)
                || (value instanceof BigDecimal && ((BigDecimal) value)
                        .compareTo(BigDecimal.valueOf(((Number) value).longValue())) == 0);
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private static boolean canConvertToFloat(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Float
                || (value instanceof Integer && (int) value == (float) (int) value)
                || (value instanceof Long && (long) value == (float) (long) value)
                || (value instanceof Double && (double) value == (float) (double) value)
                || (value instanceof BigDecimal && ((BigDecimal) value)
                        .compareTo(BigDecimal.valueOf(((Number) value).floatValue())) == 0);
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private static boolean canConvertToDouble(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Float || value instanceof Double
                || (value instanceof Long && (long) value == (double) (long) value)
                || (value instanceof BigDecimal && ((BigDecimal) value)
                        .compareTo(BigDecimal.valueOf(((Number) value).doubleValue())) == 0);
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

    private static class Converter {
        private final Predicate<Object> tester;
        private final UnaryOperator<Number> converter;

        Converter(Predicate<Object> tester, UnaryOperator<Number> converter) {
            this.tester = tester;
            this.converter = converter;
        }

        boolean canConvert(Object value) {
            return tester.test(value);
        }

        Number convert(Number value) {
            return converter.apply(value);
        }
    }
}
