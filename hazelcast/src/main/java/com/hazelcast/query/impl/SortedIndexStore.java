/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import com.hazelcast.util.NavigableSet;
import com.hazelcast.util.concurrent.ConcurrentSkipListSet;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SortedIndexStore implements IndexStore {
    private final ConcurrentMap<Long, ConcurrentMap<Long, Record>> mapRecords = new ConcurrentHashMap<Long, ConcurrentMap<Long, Record>>(100, 0.75f, 1);
    private final NavigableSet<Long> sortedSet = new ConcurrentSkipListSet<Long>();

    public void getSubRecordsBetween(MultiResultSet results, Long from, Long to) {
        Set<Long> values = sortedSet.subSet(from, to);
        for (Long value : values) {
            ConcurrentMap<Long, Record> records = mapRecords.get(value);
            if (records != null) {
                results.addResultSet(value, records.values());
            }
        }
        // to wasn't included so include now
        ConcurrentMap<Long, Record> records = mapRecords.get(to);
        if (records != null) {
            results.addResultSet(to, records.values());
        }
    }

    public void getSubRecords(MultiResultSet results, boolean equal, boolean lessThan, Long searchedValue) {
        Set<Long> values = (lessThan) ? sortedSet.headSet(searchedValue) : sortedSet.tailSet(searchedValue);
        for (Long value : values) {
            // exclude from when you have '>'
            // because from is included
            if (lessThan || equal || !value.equals(searchedValue)) {
                ConcurrentMap<Long, Record> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(value, records.values());
                }
            }
        }
        if (lessThan && equal) {
            // to wasn't included so include now
            ConcurrentMap<Long, Record> records = mapRecords.get(searchedValue);
            if (records != null) {
                results.addResultSet(searchedValue, records.values());
            }
        }
    }

    public void newRecordIndex(Long newValue, Record record) {
        long recordId = record.getId();
        ConcurrentMap<Long, Record> records = mapRecords.get(newValue);
        if (records == null) {
            records = new ConcurrentHashMap<Long, Record>(1, 0.75f, 1);
            mapRecords.put(newValue, records);
            sortedSet.add(newValue);
        }
        records.put(recordId, record);
    }

    public void removeRecordIndex(Long oldValue, Long recordId) {
        ConcurrentMap<Long, Record> records = mapRecords.get(oldValue);
        if (records != null) {
            records.remove(recordId);
            if (records.size() == 0) {
                mapRecords.remove(oldValue);
                sortedSet.remove(oldValue);
            }
        }
    }

    public Set<MapEntry> getRecords(Long value) {
        return new SingleResultSet(mapRecords.get(value));
    }

    public void getRecords(MultiResultSet results, Set<Long> values) {
        for (Long value : values) {
            ConcurrentMap<Long, Record> records = mapRecords.get(value);
            if (records != null) {
                results.addResultSet(value, records.values());
            }
        }
    }

    public ConcurrentMap<Long, ConcurrentMap<Long, Record>> getMapRecords() {
        return mapRecords;
    }

    @Override
    public String toString() {
        return "SortedIndexStore{" +
                "mapRecords=" + mapRecords.size() +
                '}';
    }
}
