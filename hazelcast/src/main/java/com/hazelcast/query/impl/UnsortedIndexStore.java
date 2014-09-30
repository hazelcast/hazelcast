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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Store indexes out of turn.
 */
public class UnsortedIndexStore extends BaseIndexStore {

    private final ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> mapRecords
            = new ConcurrentHashMap<Comparable, ConcurrentMap<Data, QueryableEntry>>(1000);

    @Override
    public void newIndex(Comparable newValue, QueryableEntry record) {
        ensureNotLocked();

        takeLock();
        try {
            Data indexKey = record.getIndexKey();
            ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(newValue);
            if (records == null) {
                records = new ConcurrentHashMap<Data, QueryableEntry>();
                ConcurrentMap<Data, QueryableEntry> existing = mapRecords.putIfAbsent(newValue, records);
                if (existing != null) {
                    records = existing;
                }
            }
            records.put(indexKey, record);
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
        } finally {
            releaseLock();
        }
    }

    @Override
    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
        ensureNotLocked();

        readerCount.incrementAndGet();
        try {
            Comparable paramFrom = from;
            Comparable paramTo = to;
            int trend = paramFrom.compareTo(paramTo);
            if (trend == 0) {
                ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(paramFrom);
                if (records != null) {
                    results.addResultSet(records);
                }
                return;
            }
            if (trend < 0) {
                Comparable oldFrom = paramFrom;
                paramFrom = to;
                paramTo = oldFrom;
            }
            Set<Comparable> values = mapRecords.keySet();
            for (Comparable value : values) {
                if (value.compareTo(paramFrom) <= 0 && value.compareTo(paramTo) >= 0) {
                    ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                    if (records != null) {
                        results.addResultSet(records);
                    }
                }
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
            Set<Comparable> values = mapRecords.keySet();
            for (Comparable value : values) {
                boolean valid;
                int result = value.compareTo(searchedValue);
                switch (comparisonType) {
                    case LESSER:
                        valid = result < 0;
                        break;
                    case LESSER_EQUAL:
                        valid = result <= 0;
                        break;
                    case GREATER:
                        valid = result > 0;
                        break;
                    case GREATER_EQUAL:
                        valid = result >= 0;
                        break;
                    case NOT_EQUAL:
                        valid = result != 0;
                        break;
                    default:
                        throw new IllegalStateException("Unrecognized comparisonType:" + comparisonType);
                }
                if (valid) {
                    ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
                    if (records != null) {
                        results.addResultSet(records);
                    }
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
        return "UnsortedIndexStore{"
                + "mapRecords=" + mapRecords.size()
                + '}';
    }
}
