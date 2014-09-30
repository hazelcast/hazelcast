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
        takeWriteLock();
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
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
        takeReadLock();
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
            releaseReadLock();
        }
    }

    @Override
    public void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue) {
        takeReadLock();
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
        return "UnsortedIndexStore{"
                + "mapRecords=" + mapRecords.size()
                + '}';
    }
}
