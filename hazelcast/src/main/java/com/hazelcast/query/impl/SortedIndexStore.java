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

import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Store indexes rankly.
 */
public class SortedIndexStore extends BaseIndexStore {

    private static final float LOAD_FACTOR = 0.75f;
    private final ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> mapRecords
            = new ConcurrentHashMap<Comparable, ConcurrentMap<Data, QueryableEntry>>(1000);
    private final NavigableSet<Comparable> sortedSet = new ConcurrentSkipListSet<Comparable>();

    @Override
    public void newIndex(Comparable newValue, QueryableEntry record) {
        takeWriteLock();
        try {
            ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(newValue);
            if (records == null) {
                records = new ConcurrentHashMap<Data, QueryableEntry>(1, LOAD_FACTOR, 1);
                mapRecords.put(newValue, records);
                if (!(newValue instanceof IndexImpl.NullObject)) {
                    sortedSet.add(newValue);
                }
            }
            records.put(record.getIndexKey(), record);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void updateIndex(Comparable oldValue, Comparable newValue, QueryableEntry entry) {
        takeWriteLock();
        try {
            removeIndex(oldValue, entry.getIndexKey());
            newIndex(newValue, entry);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void removeIndex(Comparable oldValue, Data indexKey) {
        takeWriteLock();
        try {
            ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(oldValue);
            if (records != null) {
                records.remove(indexKey);
                if (records.size() == 0) {
                    mapRecords.remove(oldValue);
                    sortedSet.remove(oldValue);
                }
            }
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            mapRecords.clear();
            sortedSet.clear();
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
        takeReadLock();
        try {
            Set<Comparable> values = sortedSet.subSet(from, to);
            for (Comparable value : values) {
                ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(records);
                }
            }
            // to wasn't included so include now
            ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(to);
            if (records != null) {
                results.addResultSet(records);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue) {
        takeReadLock();
        try {
            Set<Comparable> values;
            boolean notEqual = false;
            switch (comparisonType) {
                case LESSER:
                    values = sortedSet.headSet(searchedValue, false);
                    break;
                case LESSER_EQUAL:
                    values = sortedSet.headSet(searchedValue, true);
                    break;
                case GREATER:
                    values = sortedSet.tailSet(searchedValue, false);
                    break;
                case GREATER_EQUAL:
                    values = sortedSet.tailSet(searchedValue, true);
                    break;
                case NOT_EQUAL:
                    values = sortedSet;
                    notEqual = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized comparisonType:" + comparisonType);
            }

            for (Comparable value : values) {
                if (notEqual && searchedValue.equals(value)) {
                    // skip this value if predicateType is NOT_EQUAL
                    continue;
                }
                ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(records);
                }
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        takeReadLock();
        try {
            return mapRecords.get(indexValue);
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            return new SingleResultSet(mapRecords.get(value));
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public void getRecords(MultiResultSet results, Set<Comparable> values) {
        takeReadLock();
        try {
            for (Comparable value : values) {
                ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(records);
                }
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public String toString() {
        return "SortedIndexStore{"
                + "mapRecords=" + mapRecords.size()
                + '}';
    }
}
