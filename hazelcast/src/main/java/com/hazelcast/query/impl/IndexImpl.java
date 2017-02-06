/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

public class IndexImpl implements Index {

    public static final NullObject NULL = new NullObject();

    private final IndexStore indexStore;
    private final String attributeName;
    private final boolean ordered;

    private volatile TypeConverter converter;

    private final InternalSerializationService ss;
    private final Extractors extractors;

    public IndexImpl(String attributeName, boolean ordered, InternalSerializationService ss, Extractors extractors) {
        this.attributeName = attributeName;
        this.ordered = ordered;
        this.ss = ss;
        this.indexStore = ordered ? new SortedIndexStore() : new UnsortedIndexStore();
        this.extractors = extractors;
    }

    @Override
    public void saveEntryIndex(QueryableEntry entry, Object oldRecordValue) throws QueryException {
        /*
         * At first, check if converter is not initialized, initialize it before saving an entry index
         * Because, if entity index is saved before,
         * that thread can be blocked before executing converter setting code block,
         * another thread can query over indexes without knowing the converter and
         * this causes to class cast exceptions.
         */
        if (converter == null || converter == NULL_CONVERTER) {
            converter = entry.getConverter(attributeName);
        }

        Object newAttributeValue = extractAttributeValue(entry.getKeyData(), entry.getTargetObject(false));
        if (oldRecordValue == null) {
            indexStore.newIndex(newAttributeValue, entry);
        } else {
            Object oldAttributeValue = extractAttributeValue(entry.getKeyData(), oldRecordValue);
            indexStore.updateIndex(oldAttributeValue, newAttributeValue, entry);
        }
    }

    @Override
    public void removeEntryIndex(Data key, Object value) {
        Object attributeValue = extractAttributeValue(key, value);
        indexStore.removeIndex(attributeValue, key);
    }

    private Object extractAttributeValue(Data key, Object value) {
        return QueryableEntry.extractAttributeValue(extractors, ss, attributeName, key, value);
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            return getRecords(values[0]);
        } else {
            if (converter != null) {
                Set<Comparable> convertedValues = new HashSet<Comparable>(values.length);
                for (Comparable value : values) {
                    convertedValues.add(convert(value));
                }
                return indexStore.getRecords(convertedValues);
            }
            return Collections.EMPTY_SET;
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable attributeValue) {
        if (converter == null) {
            return new SingleResultSet(null);
        }
        return indexStore.getRecords(convert(attributeValue));
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedAttributeValue) {
        if (converter == null) {
            return Collections.EMPTY_SET;
        }
        return indexStore.getSubRecords(comparisonType, convert(searchedAttributeValue));
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable fromAttributeValue, Comparable toAttributeValue) {
        if (converter == null) {
            return Collections.EMPTY_SET;
        }
        return indexStore.getSubRecordsBetween(convert(fromAttributeValue), convert(toAttributeValue));
    }

    /**
     * Note: the fact that the given attributeValue is of type Comparable doesn't mean that this value is of the same
     * type as the one that's stored in the index, thus the conversion is needed.
     *
     * @param attributeValue to be converted from given type to the type of the attribute that's stored in the index
     * @return converted value that may be compared with the value that's stored in the index
     */
    private Comparable convert(Comparable attributeValue) {
        return converter.convert(attributeValue);
    }

    /**
     * Provides comparable null object.
     */
    @Override
    public TypeConverter getConverter() {
        return converter;
    }

    @Override
    public void clear() {
        indexStore.clear();
        converter = null;
    }

    @Override
    public String getAttributeName() {
        return attributeName;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    public static final class NullObject implements Comparable, IdentifiedDataSerializable {
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

        @Override
        public int getFactoryId() {
            return PredicateDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return PredicateDataSerializerHook.NULL_OBJECT;
        }
    }
}
