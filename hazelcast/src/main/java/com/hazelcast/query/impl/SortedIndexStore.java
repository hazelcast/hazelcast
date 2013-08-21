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

public class SortedIndexStore implements IndexStore {
    private final ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> mapRecords = new ConcurrentHashMap<Comparable, ConcurrentMap<Data, QueryableEntry>>(1000);
    private final NavigableSet<Comparable> sortedSet = new ConcurrentSkipListSet<Comparable>();

    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
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
    }

    public void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue) {
        Set<Comparable> values = null;
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
        }
        if (values != null) {
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
        }
    }

    public void newIndex(Comparable newValue, QueryableEntry record) {
        ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(newValue);
        if (records == null) {
            records = new ConcurrentHashMap<Data, QueryableEntry>(1, 0.75f, 1);
            mapRecords.put(newValue, records);
            sortedSet.add(newValue);
        }
        records.put(record.getIndexKey(), record);
    }

    public ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        return mapRecords.get(indexValue);
    }

    public void clear() {
        mapRecords.clear();
        sortedSet.clear();
    }

    public void removeIndex(Comparable oldValue, Data indexKey) {
        ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(oldValue);
        if (records != null) {
            records.remove(indexKey);
            if (records.size() == 0) {
                mapRecords.remove(oldValue);
                sortedSet.remove(oldValue);
            }
        }
    }

    public Set<QueryableEntry> getRecords(Comparable value) {
        return new SingleResultSet(mapRecords.get(value));
    }

    public void getRecords(MultiResultSet results, Set<Comparable> values) {
        for (Comparable value : values) {
            ConcurrentMap<Data, QueryableEntry> records = mapRecords.get(value);
            if (records != null) {
                results.addResultSet(records);
            }
        }
    }

    @Override
    public String toString() {
        return "SortedIndexStore{" +
                "mapRecords=" + mapRecords.size() +
                '}';
    }
}
