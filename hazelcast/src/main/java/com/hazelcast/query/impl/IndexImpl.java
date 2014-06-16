/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class IndexImpl implements Index {
    public static final NullObject NULL = new NullObject();

    // indexKey -- indexValue
    private final ConcurrentMap<Data, Comparable> recordValues = new ConcurrentHashMap<Data, Comparable>(1000);
    private final IndexStore indexStore;
    private final String attribute;
    private final boolean ordered;

    private volatile AttributeType attributeType;

    public IndexImpl(String attribute, boolean ordered) {
        this.attribute = attribute;
        this.ordered = ordered;
        indexStore = (ordered) ? new SortedIndexStore() : new UnsortedIndexStore();
    }

    @Override
    public void removeEntryIndex(Data indexKey) {
        Comparable oldValue = recordValues.remove(indexKey);
        if (oldValue != null) {
            indexStore.removeIndex(oldValue, indexKey);
        }
    }

    @Override
    public void clear() {
        recordValues.clear();
        indexStore.clear();
    }

    ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        return indexStore.getRecordMap(indexValue);
    }

    @Override
    public void saveEntryIndex(QueryableEntry e) throws QueryException {
        Data key = e.getIndexKey();
        Comparable oldValue = recordValues.remove(key);
        Comparable newValue = e.getAttribute(attribute);
        if (newValue == null) {
            newValue = NULL;
        } else if (newValue.getClass().isEnum()) {
            newValue = TypeConverters.ENUM_CONVERTER.convert(newValue);
        }
        recordValues.put(key, newValue);
        if (oldValue == null) {
            // new
            indexStore.newIndex(newValue, e);
        } else {
            // update
            indexStore.removeIndex(oldValue, key);
            indexStore.newIndex(newValue, e);
        }
        if (attributeType == null) {
            attributeType = e.getAttributeType(attribute);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            return indexStore.getRecords(convert(values[0]));
        } else {
            Set<Comparable> convertedValues = new HashSet<Comparable>(values.length);
            for (Comparable value : values) {
                convertedValues.add(convert(value));
            }
            MultiResultSet results = new MultiResultSet();
            indexStore.getRecords(results, convertedValues);
            return results;
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return indexStore.getRecords(convert(value));
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        MultiResultSet results = new MultiResultSet();
        indexStore.getSubRecordsBetween(results, convert(from), convert(to));
        return results;
    }

    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        MultiResultSet results = new MultiResultSet();
        indexStore.getSubRecords(results, comparisonType, convert(searchedValue));
        return results;
    }

    private Comparable convert(Comparable value) {
        if (attributeType == null) return value;
        return attributeType.getConverter().convert(value);
    }

    public ConcurrentMap<Data, Comparable> getRecordValues() {
        return recordValues;
    }

    @Override
    public String getAttributeName() {
        return attribute;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    public final static class NullObject implements Comparable {
        @Override
        public int compareTo(Object o) {
            if (o == this || o instanceof NullObject) return 0;
            return -1;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }
    }
}
