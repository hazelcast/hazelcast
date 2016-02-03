/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * Store indexes rankly.
 */
public class SortedIndexStore extends BaseIndexStore {

    private final ConcurrentMap<Data, QueryableEntry> recordsWithNullValue
            = new ConcurrentHashMap<Data, QueryableEntry>();

    private final ConcurrentSkipListMap<Comparable, ConcurrentMap<Data, QueryableEntry>> recordMap
            = new ConcurrentSkipListMap<Comparable, ConcurrentMap<Data, QueryableEntry>>();

    @Override
    void newIndexInternal(Comparable newValue, QueryableEntry record) {
        if (newValue instanceof IndexImpl.NullObject) {
            recordsWithNullValue.put(record.getKeyData(), record);
        } else {
            mapAttributeToEntry(newValue, record);
        }
    }

    private void mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        ConcurrentMap<Data, QueryableEntry> records = recordMap.get(attribute);
        if (records == null) {
            records = new ConcurrentHashMap<Data, QueryableEntry>(1, LOAD_FACTOR, 1);
            recordMap.put(attribute, records);
        }
        records.put(entry.getKeyData(), entry);
    }

    @Override
    void removeIndexInternal(Comparable oldValue, Data indexKey) {
        if (oldValue instanceof IndexImpl.NullObject) {
            recordsWithNullValue.remove(indexKey);
        } else {
            removeMappingForAttribute(oldValue, indexKey);
        }
    }

    private void removeMappingForAttribute(Object attribute, Data indexKey) {
        ConcurrentMap<Data, QueryableEntry> records = recordMap.get(attribute);
        if (records != null) {
            records.remove(indexKey);
            if (records.size() == 0) {
                recordMap.remove(attribute);
            }
        }
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            recordsWithNullValue.clear();
            recordMap.clear();
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            SortedMap<Comparable, ConcurrentMap<Data, QueryableEntry>> subMap =
                    recordMap.subMap(from, true, to, true);
            for (ConcurrentMap<Data, QueryableEntry> value : subMap.values()) {
                results.addResultSet(value);
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            SortedMap<Comparable, ConcurrentMap<Data, QueryableEntry>> subMap;
            switch (comparisonType) {
                case LESSER:
                    subMap = recordMap.headMap(searchedValue, false);
                    break;
                case LESSER_EQUAL:
                    subMap = recordMap.headMap(searchedValue, true);
                    break;
                case GREATER:
                    subMap = recordMap.tailMap(searchedValue, false);
                    break;
                case GREATER_EQUAL:
                    subMap = recordMap.tailMap(searchedValue, true);
                    break;
                case NOT_EQUAL:
                    // TODO There maybe more efficient way such as
                    // Make a copy of current record map and just remove searched value.
                    // So remaining records are not equal to searched value
                    for (Map.Entry<Comparable, ConcurrentMap<Data, QueryableEntry>> entry : recordMap.entrySet()) {
                        if (!searchedValue.equals(entry.getKey())) {
                            results.addResultSet(entry.getValue());
                        }
                    }
                    return results;
                default:
                    throw new IllegalArgumentException("Unrecognized comparisonType: " + comparisonType);
            }
            for (ConcurrentMap<Data, QueryableEntry> value : subMap.values()) {
                results.addResultSet(value);
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable value) {
        takeReadLock();
        try {
            if (value instanceof IndexImpl.NullObject) {
                return recordsWithNullValue;
            } else {
                return recordMap.get(value);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value instanceof IndexImpl.NullObject) {
                return new SingleResultSet(recordsWithNullValue);
            } else {
                return new SingleResultSet(recordMap.get(value));
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            for (Comparable value : values) {
                ConcurrentMap<Data, QueryableEntry> records;
                if (value instanceof IndexImpl.NullObject) {
                    records = recordsWithNullValue;
                } else {
                    records = recordMap.get(value);
                }
                if (records != null) {
                    results.addResultSet(records);
                }
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public String toString() {
        return "SortedIndexStore{"
                + "recordMap=" + recordMap.size()
                + '}';
    }
}
