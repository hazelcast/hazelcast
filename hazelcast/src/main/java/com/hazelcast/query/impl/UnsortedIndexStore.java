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

import com.hazelcast.core.MapEntry;
import com.hazelcast.query.PredicateType;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class UnsortedIndexStore implements IndexStore {
    private final ConcurrentMap<Long, ConcurrentMap<Long, IndexEntry>> mapRecords = new ConcurrentHashMap<Long, ConcurrentMap<Long, IndexEntry>>(100, 0.75f, 1);

    public void getSubRecordsBetween(MultiResultSet results, Long from, Long to) {
        Set<Long> values = mapRecords.keySet();
        for (Long value : values) {
            if (value >= from && value <= to) {
                ConcurrentMap<Long, IndexEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(value, records.values());
                }
            }
        }
    }

    public void getSubRecords(MultiResultSet results, PredicateType predicateType, Long searchedValue) {
        Set<Long> values = mapRecords.keySet();
        for (Long value : values) {
            boolean valid = false;
            switch (predicateType) {
                case LESSER:
                    valid = value < searchedValue;
                    break;
                case LESSER_EQUAL:
                    valid = value <= searchedValue;
                    break;
                case GREATER:
                    valid = value > searchedValue;
                    break;
                case GREATER_EQUAL:
                    valid = value >= searchedValue;
                    break;
                case NOT_EQUAL:
                    valid = value.longValue() != searchedValue.longValue();
                    break;
            }
            if (valid) {
                ConcurrentMap<Long, IndexEntry> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(value, records.values());
                }
            }
        }
    }

    public void newRecordIndex(Long newValue, IndexEntry record) {
        Long recordId = record.getId();
        ConcurrentMap<Long, IndexEntry> records = mapRecords.get(newValue);
        if (records == null) {
            records = new ConcurrentHashMap<Long, IndexEntry>(1, 0.75f, 1);
            mapRecords.put(newValue, records);
        }
        records.put(recordId, record);
    }

    public void removeRecordIndex(Long oldValue, Long recordId) {
        ConcurrentMap<Long, IndexEntry> records = mapRecords.get(oldValue);
        if (records != null) {
            records.remove(recordId);
            if (records.size() == 0) {
                mapRecords.remove(oldValue);
            }
        }
    }

    public Set<MapEntry> getRecords(Long value) {
        return new SingleResultSet(mapRecords.get(value));
    }

    public void getRecords(MultiResultSet results, Set<Long> values) {
        for (Long value : values) {
            ConcurrentMap<Long, IndexEntry> records = mapRecords.get(value);
            if (records != null) {
                results.addResultSet(value, records.values());
            }
        }
    }

    @Override
    public String toString() {
        return "UnsortedIndexStore{" +
                "mapRecords=" + mapRecords.size() +
                '}';
    }
}