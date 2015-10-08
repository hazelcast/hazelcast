/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.QueryException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;

/**
 * Implementation for {@link com.hazelcast.query.impl.Index}
 */
public class IndexImpl implements Index {

    /**
     * Creates instance from NullObject is a inner class.
     */
    public static final NullObject NULL = new NullObject();

    // indexKey -- indexValue
    private final IndexStore indexStore;
    private final String attribute;
    private final boolean ordered;

    private volatile TypeConverter converter;

    private final SerializationService ss;

    public IndexImpl(String attribute, boolean ordered, SerializationService ss) {
        this.attribute = attribute;
        this.ordered = ordered;
        this.ss = ss;
        this.indexStore = ordered ? new SortedIndexStore() : new UnsortedIndexStore();
    }

    @Override
    public TypeConverter getConverter() {
        return converter;
    }

    @Override
    public void removeEntryIndex(Record record) {
        Comparable value = QueryEntryUtils.extractAttribute(attribute, record.getKey(), record.getValue(), ss);

        if (value.getClass().isEnum()) {
            value = TypeConverters.ENUM_CONVERTER.convert(value);
        }

        if (value != null) {
            indexStore.removeIndex(value, record.getKey());
        }
    }

    @Override
    public void clear() {
        indexStore.clear();
        // Clear converter
        converter = null;
    }

    ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        return indexStore.getRecordMap(indexValue);
    }

    @Override
    public void saveEntryIndex(QueryableEntry e, Object oldRecordValue) throws QueryException {
        /*
         * At first, check if converter is not initialized, initialize it before saving an entry index
         * Because, if entity index is saved before,
         * that thread can be blocked before executing converter setting code block,
         * another thread can query over indexes without knowing the converter and
         * this causes to class cast exceptions.
         */
        if (converter == null) {
            // Initialize attribute type by using entry index
            AttributeType attributeType = e.getAttributeType(attribute);
            converter = attributeType == null ? IDENTITY_CONVERTER : attributeType.getConverter();
        }

        Comparable oldValue = null;
        if (oldRecordValue != null) {
            oldValue = QueryEntryUtils.extractAttribute(attribute, e.getKey(), oldRecordValue, ss);
            oldValue = sanitizeValue(oldValue);
        }

        Comparable newValue = QueryEntryUtils.extractAttribute(attribute, e.getKey(), e.getValue(), ss);
        newValue = sanitizeValue(newValue);

        if (oldValue == null) {
            // new
            indexStore.newIndex(newValue, e);
        } else {
            // update
            indexStore.updateIndex(oldValue, newValue, e);
        }
    }

    private static Comparable sanitizeValue(Comparable value) {
        if (value == null) {
            return NULL;
        }
        if (value.getClass().isEnum()) {
            return TypeConverters.ENUM_CONVERTER.convert(value);
        }
        return value;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            if (converter != null) {
                return indexStore.getRecords(convert(values[0]));
            } else {
                return new SingleResultSet(null);
            }
        } else {
            MultiResultSet results = new MultiResultSet();
            if (converter != null) {
                Set<Comparable> convertedValues = new HashSet<Comparable>(values.length);
                for (Comparable value : values) {
                    convertedValues.add(convert(value));
                }
                indexStore.getRecords(results, convertedValues);
            }
            return results;
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        if (converter != null) {
            return indexStore.getRecords(convert(value));
        } else {
            return new SingleResultSet(null);
        }
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        MultiResultSet results = new MultiResultSet();
        if (converter != null) {
            indexStore.getSubRecordsBetween(results, convert(from), convert(to));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        MultiResultSet results = new MultiResultSet();
        if (converter != null) {
            indexStore.getSubRecords(results, comparisonType, convert(searchedValue));
        }
        return results;
    }

    private Comparable convert(Comparable value) {
        return converter.convert(value);
    }

    @Override
    public String getAttributeName() {
        return attribute;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Provides comparable null object.
     */
    public static final class NullObject implements Comparable, DataSerializable {
        @Override
        public int compareTo(Object o) {
            if (o == this || o instanceof NullObject) {
                return 0;
            }
            return -1;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return true;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }
    }
}
