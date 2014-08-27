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

import com.hazelcast.monitor.impl.MapIndexStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.resultset.MultiResultSet;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation for {@link com.hazelcast.query.impl.Index}
 */
public class IndexImpl implements Index {
    /**
     * Creates instance from NullObject is a inner class.
     */
    public static final NullObject NULL = new NullObject();

    // indexKey -- indexValue
    private final ConcurrentMap<Data, Comparable> recordValues = new ConcurrentHashMap<Data, Comparable>(1000);
    private final IndexStore indexStore;
    private final String attribute;
    private final boolean ordered;

    private final Predicate predicate;

    private volatile AttributeType attributeType;
    private MapIndexStats.IndexUsageIncrementer indexUsageIncrementer;

    public IndexImpl(String attribute, boolean ordered, Predicate predicate) {
        this.attribute = attribute;
        this.ordered = ordered;
        indexStore = (ordered) ? new SortedIndexStore() : new UnsortedIndexStore();
        this.predicate = predicate;
    }

    public IndexImpl(String attribute, boolean ordered) {
        this(attribute, ordered, null);
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
        // Clear attribute type
        attributeType = null;
    }

    ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        return indexStore.getRecordMap(indexValue);
    }

    @Override
    public void saveEntryIndex(QueryableEntry e) throws QueryException {
        /*
         * At first, check if attribute type is not initialized, initialize it before saving an entry index
         * Because, if entity index is saved before,
         * that thread can be blocked before executing attribute type setting code block,
         * another thread can query over indexes without knowing attribute type and
         * this causes to class cast exceptions.
         */
        if (attributeType == null) {
            // Initialize attribute type by using entry index
            attributeType = e.getAttributeType(attribute);
        }
        if (predicate != null && !predicate.apply(e)) {
            return;
        }

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
    }

    @Override
    public Predicate getPredicate() {
        return predicate;
    }

    private void onIndexUsage() {
        if (indexUsageIncrementer != null) {
            indexUsageIncrementer.incrementIndexUsage();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            if (attributeType != null) {
                Set<QueryableEntry> records = indexStore.getRecords(convert(values[0]));
                onIndexUsage();
                return records;
            } else {
                return new SingleResultSet(null);
            }
        } else {
            MultiResultSet results = new MultiResultSet();
            if (attributeType != null) {
                Set<Comparable> convertedValues = new HashSet<Comparable>(values.length);
                for (Comparable value : values) {
                    convertedValues.add(convert(value));
                }
                indexStore.getRecords(results, convertedValues);
                onIndexUsage();
            }
            return results;
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        if (attributeType != null) {
            Set<QueryableEntry> records = indexStore.getRecords(convert(value));
            onIndexUsage();
            return records;
        } else {
            return new SingleResultSet(null);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords() {
        if (attributeType != null) {
            Set<QueryableEntry> records = indexStore.getRecords();
            onIndexUsage();
            return records;
        } else {
            return new SingleResultSet(null);
        }
    }

    @Override
    public long getRecordCount() {
        return indexStore.getRecordCount();
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        MultiResultSet results = new MultiResultSet();
        if (attributeType != null) {
            indexStore.getSubRecordsBetween(results, convert(from), convert(to));
            onIndexUsage();
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        MultiResultSet results = new MultiResultSet();
        if (attributeType != null) {
            indexStore.getSubRecords(results, comparisonType, convert(searchedValue));
            onIndexUsage();
        }
        return results;
    }

    private Comparable convert(Comparable value) {
        if (attributeType == null) {
            return value;
        }
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

    @Override
    public void setStatistics(MapIndexStats.IndexUsageIncrementer localMapIndexStats) {
        this.indexUsageIncrementer = localMapIndexStats;
    }

    /**
     * Provides comparable null object.
     */
    public static final class NullObject implements Comparable {
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
    }
}
