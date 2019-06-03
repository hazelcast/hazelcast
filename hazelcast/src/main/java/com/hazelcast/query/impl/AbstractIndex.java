/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.monitor.impl.IndexOperationStats;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Set;

import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.util.Collections.emptySet;

/**
 * Provides an abstract base for indexes.
 */
public abstract class AbstractIndex implements InternalIndex {

    /**
     * Represents a null-like value that is equal to itself and less than any
     * other value except {@link CompositeValue#NEGATIVE_INFINITY}. The latter
     * is needed to establish the ordering of keys for composite indexes.
     */
    public static final ComparableIdentifiedDataSerializable NULL = new NullObject();

    protected final InternalSerializationService ss;
    protected final Extractors extractors;
    protected final IndexStore indexStore;
    protected final IndexCopyBehavior copyBehavior;

    private final String name;
    private final String[] components;
    private final boolean ordered;
    private final PerIndexStats stats;

    private volatile TypeConverter converter;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public AbstractIndex(String name, String[] components, boolean ordered, InternalSerializationService ss,
                         Extractors extractors, IndexCopyBehavior copyBehavior, PerIndexStats stats) {
        this.name = name;
        this.components = components;
        this.ordered = ordered;
        this.ss = ss;
        this.extractors = extractors;
        this.copyBehavior = copyBehavior;
        this.indexStore = createIndexStore(ordered, stats);
        this.stats = stats;
    }

    protected abstract IndexStore createIndexStore(boolean ordered, PerIndexStats stats);

    @Override
    public String getName() {
        return name;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public String[] getComponents() {
        return components;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public TypeConverter getConverter() {
        return converter;
    }

    @Override
    public void putEntry(QueryableEntry entry, Object oldValue, OperationSource operationSource) {
        long timestamp = stats.makeTimestamp();
        IndexOperationStats operationStats = stats.createOperationStats();

        /*
         * At first, check if converter is not initialized, initialize it before
         * saving an entry index. Because, if entity index is saved before, that
         * thread can be blocked before executing converter setting code block,
         * another thread can query over indexes without knowing the converter
         * and this causes to class cast exceptions.
         */
        if (converterIsUnassignedOrTransient(converter)) {
            converter = obtainConverter(entry);
        }

        Object newAttributeValue = extractAttributeValue(entry.getKeyData(), entry.getTargetObject(false));
        if (oldValue == null) {
            indexStore.insert(newAttributeValue, entry, operationStats);
            stats.onInsert(timestamp, operationStats, operationSource);
        } else {
            Object oldAttributeValue = extractAttributeValue(entry.getKeyData(), oldValue);
            indexStore.update(oldAttributeValue, newAttributeValue, entry, operationStats);
            stats.onUpdate(timestamp, operationStats, operationSource);
        }
    }

    @Override
    public void removeEntry(Data key, Object value, OperationSource operationSource) {
        long timestamp = stats.makeTimestamp();
        IndexOperationStats operationStats = stats.createOperationStats();

        Object attributeValue = extractAttributeValue(key, value);
        indexStore.remove(attributeValue, key, operationStats);
        stats.onRemove(timestamp, operationStats, operationSource);
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        long timestamp = stats.makeTimestamp();

        if (converter == null) {
            stats.onIndexHit(timestamp, 0);
            return emptySet();
        }

        Set<QueryableEntry> result = indexStore.getRecords(convert(value));
        stats.onIndexHit(timestamp, result.size());
        return result;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            return getRecords(values[0]);
        }

        long timestamp = stats.makeTimestamp();

        if (converter == null || values.length == 0) {
            stats.onIndexHit(timestamp, 0);
            return emptySet();
        }

        Set<Comparable> convertedValues = createHashSet(values.length);
        for (Comparable value : values) {
            Comparable converted = convert(value);
            convertedValues.add(canonicalizeQueryArgumentScalar(converted));
        }
        Set<QueryableEntry> result = indexStore.getRecords(convertedValues);
        stats.onIndexHit(timestamp, result.size());
        return result;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        long timestamp = stats.makeTimestamp();

        if (converter == null) {
            stats.onIndexHit(timestamp, 0);
            return emptySet();
        }

        Set<QueryableEntry> result = indexStore.getRecords(convert(from), fromInclusive, convert(to), toInclusive);
        stats.onIndexHit(timestamp, result.size());
        return result;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        long timestamp = stats.makeTimestamp();

        if (converter == null) {
            stats.onIndexHit(timestamp, 0);
            return emptySet();
        }

        Set<QueryableEntry> result = indexStore.getRecords(comparison, convert(value));
        stats.onIndexHit(timestamp, result.size());
        return result;
    }

    @Override
    public void clear() {
        indexStore.clear();
        converter = null;
        stats.onClear();
    }

    @Override
    public void destroy() {
        stats.onClear();
    }

    @Override
    public final Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        return indexStore.canonicalizeQueryArgumentScalar(value);
    }

    @Override
    public PerIndexStats getPerIndexStats() {
        return stats;
    }

    private Object extractAttributeValue(Data key, Object value) {
        if (components == null) {
            return QueryableEntry.extractAttributeValue(extractors, ss, name, key, value, null);
        } else {
            Comparable[] valueComponents = new Comparable[components.length];
            for (int i = 0; i < components.length; ++i) {
                Object extractedValue = QueryableEntry.extractAttributeValue(extractors, ss, components[i], key, value, null);
                if (extractedValue instanceof MultiResult) {
                    throw new IllegalStateException(
                            "Collection/array attributes are not supported by composite indexes: " + components[i]);
                } else if (extractedValue == null || extractedValue instanceof Comparable) {
                    valueComponents[i] = (Comparable) extractedValue;
                } else {
                    throw new IllegalStateException("Unsupported non-comparable value type: " + extractedValue.getClass());
                }
            }
            return new CompositeValue(valueComponents);
        }
    }

    /**
     * Note: the fact that the given value is of type Comparable doesn't mean
     * that this value is of the same type as the one that's stored in the index,
     * thus the conversion is needed.
     *
     * @param value to be converted from given type to the type of the
     *              attribute that's stored in the index
     * @return converted value that may be compared with the value that's stored
     * in the index
     */
    private Comparable convert(Comparable value) {
        return converter.convert(value);
    }

    private TypeConverter obtainConverter(QueryableEntry entry) {
        if (components == null) {
            return entry.getConverter(name);
        } else {
            CompositeConverter existingConverter = (CompositeConverter) converter;
            TypeConverter[] converters = new TypeConverter[components.length];
            for (int i = 0; i < components.length; ++i) {
                TypeConverter existingComponentConverter = getNonTransientComponentConverter(existingConverter, i);
                if (existingComponentConverter == null) {
                    converters[i] = entry.getConverter(components[i]);
                    assert converters[i] != null;
                } else {
                    // preserve the old one to avoid downgrading
                    converters[i] = existingComponentConverter;
                }
            }

            return new CompositeConverter(converters);
        }
    }

    private static boolean converterIsUnassignedOrTransient(TypeConverter converter) {
        if (converter == null) {
            // unassigned
            return true;
        }

        if (converter == NULL_CONVERTER) {
            // transient
            return true;
        }

        if (!(converter instanceof CompositeConverter)) {
            return false;
        }
        CompositeConverter compositeConverter = (CompositeConverter) converter;
        return compositeConverter.isTransient();
    }

    private static TypeConverter getNonTransientComponentConverter(CompositeConverter converter, int index) {
        if (converter == null) {
            return null;
        }

        TypeConverter componentConverter = converter.getComponentConverter(index);
        return componentConverter == NULL_CONVERTER ? null : componentConverter;
    }

    private static final class NullObject implements ComparableIdentifiedDataSerializable {

        @SuppressWarnings("NullableProblems")
        @Override
        public int compareTo(Object o) {
            if (this == o) {
                return 0;
            }
            return o == NEGATIVE_INFINITY ? +1 : -1;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public String toString() {
            return "NULL";
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            // nothing to serialize
        }

        @Override
        public void readData(ObjectDataInput in) {
            // nothing to deserialize
        }

        @Override
        public int getFactoryId() {
            return PredicateDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return PredicateDataSerializerHook.NULL_OBJECT;
        }

    }

}
