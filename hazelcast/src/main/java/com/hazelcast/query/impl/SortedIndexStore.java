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
        ensureNotLocked();

        takeLock();
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
            releaseLock();
        }
    }

    @Override
    public void updateIndex(Comparable oldValue, Comparable newValue, QueryableEntry entry) {
        ensureNotLocked();

        takeLock();
        try {
            removeIndex(oldValue, entry.getIndexKey());
            newIndex(newValue, entry);
        } finally {
            releaseLock();
        }
    }

    @Override
    public void removeIndex(Comparable oldValue, Data indexKey) {
        ensureNotLocked();

        takeLock();
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
            releaseLock();
        }
    }

    @Override
    public void clear() {
        ensureNotLocked();

        takeLock();
        try {
            mapRecords.clear();
            sortedSet.clear();
        } finally {
            releaseLock();
        }
    }

    @Override
    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
        ensureNotLocked();

        readerCount.incrementAndGet();
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
            readerCount.decrementAndGet();
        }
    }

    @Override
    public void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue) {
        ensureNotLocked();

        readerCount.incrementAndGet();
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
            readerCount.decrementAndGet();
        }
    }

    @Override
    public ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        ensureNotLocked();

        readerCount.incrementAndGet();
        try {
            return mapRecords.get(indexValue);
        } finally {
            readerCount.decrementAndGet();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        ensureNotLocked();

        readerCount.incrementAndGet();
        try {
            return new SingleResultSet(mapRecords.get(value));
        } finally {
            readerCount.decrementAndGet();
        }
    }

    @Override
    public void getRecords(MultiResultSet results, Set<Comparable> values) {
        ensureNotLocked();

        readerCount.incrementAndGet();
        try {
            for (Comparable value : values) {
                ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(records);
                }
            }
        } finally {
            readerCount.decrementAndGet();
        }
    }

    @Override
    public String toString() {
        return "SortedIndexStore{"
                + "mapRecords=" + mapRecords.size()
                + '}';
    }
}
