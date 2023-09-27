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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.Field;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.findIndex;
import static com.hazelcast.sql.impl.expression.ConstantExpression.UNSET;
import static java.util.Map.entry;
import static java.util.stream.Collectors.toList;

public abstract class UpsertTarget {
    private final Extractors extractors;

    public UpsertTarget(InternalSerializationService serializationService) {
        extractors = serializationService != null ? Extractors.newBuilder(serializationService).build() : null;
    }

    public UpsertTarget() {
        this(null);
    }

    public UpsertConverter createConverter(List<TableField> fields) {
        Converter<?> converter = createConverter(getFields(fields));
        int topLevelIndex = findIndex(fields, field -> ((MapTableField) field).getPath().isTopLevel());
        return row -> {
            // If the top-level path (__key/this) is set, pass it as the record. Otherwise,
            // pass the RowValue object as the record by removing the top-level path.
            Object topLevel = topLevelIndex != -1 ? row.getValues().remove(topLevelIndex) : UNSET;
            return converter.apply(topLevel != UNSET ? topLevel : row);
        };
    }

    protected abstract Converter<?> createConverter(Stream<Field> fields);

    protected <T> Injector<T> createRecordInjector(Stream<Field> fields,
                                                   Function<Field, Injector<T>> createFieldInjector) {
        List<Entry<String, Injector<T>>> injectors = fields
                .map(field -> entry(field.name(), createFieldInjector.apply(field)))
                .collect(toList());
        return (record, value) -> {
            Extractor extractor = getExtractor(value);
            for (int i = 0; i < injectors.size(); i++) {
                injectors.get(i).getValue().set(record, extractor.get(i, injectors.get(i).getKey()));
            }
        };
    }

    private Extractor getExtractor(Object value) {
        if (value instanceof RowValue) {
            List<Object> values = ((RowValue) value).getValues();
            return (i, name) -> values.get(i);
        } else if (value instanceof Map) {
            return (i, name) -> ((Map<?, ?>) value).get(name);
        } else if (value instanceof GenericRecord) {
            return (i, name) -> AvroQueryTarget.extractValue((GenericRecord) value, name);
        } else {
            return (i, name) -> extractors.extract(value, name, null);
        }
    }

    /** {@link TableField} counterpart of {@link KvMetadataResolver#getFields(Map)}. */
    private static Stream<Field> getFields(List<TableField> fields) {
        return flatMap(fields, KvMetadataResolver::getFields, () -> fields.stream()
                .filter(field -> !((MapTableField) field).getPath().isTopLevel()).map(Field::new));
    }

    /** {@link TableField} counterpart of {@link KvMetadataResolver#flatMap}. */
    private static <T> T flatMap(
            List<TableField> fields,
            Function<QueryDataType, T> typeMapper,
            Supplier<T> orElse
    ) {
        if (fields.size() == 1) {
            MapTableField field = (MapTableField) fields.get(0);
            if (field.getPath().isTopLevel() && field.getType().isCustomType()) {
                return typeMapper.apply(field.getType());
            }
        }
        return orElse.get();
    }

    @FunctionalInterface
    private interface Extractor {
        Object get(int index, String name);
    }

    @FunctionalInterface
    protected interface Injector<T> {
        void set(T record, Object value);
    }

    @FunctionalInterface
    protected interface InjectorEx<T> {
        void set(T record, Object value) throws Exception;
    }

    @FunctionalInterface
    protected interface Converter<T> extends Function<Object, T> { }
}
