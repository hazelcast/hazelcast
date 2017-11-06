/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Store indexes out of turn.
 */
public class UnsortedIndexStore extends BaseIndexStore {

    private volatile Map<Data, QueryableEntry> recordsWithNullValue;

    private final ConcurrentMap<Comparable, Map<Data, QueryableEntry>> recordMap
            = new ConcurrentHashMap<Comparable, Map<Data, QueryableEntry>>(1000);

    private final IndexFunctor<Comparable, QueryableEntry> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    public UnsortedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn);
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
            addFunctor = new CopyOnWriteAddFunctor();
            removeFunctor = new CopyOnWriteRemoveFunctor();
            recordsWithNullValue = Collections.emptyMap();
        } else {
            addFunctor = new AddFunctor();
            removeFunctor = new RemoveFunctor();
            recordsWithNullValue = new ConcurrentHashMap<Data, QueryableEntry>();
        }
    }

    @Override
    void newIndexInternal(Comparable newValue, QueryableEntry record) {
        addFunctor.invoke(newValue, record);
    }

    @Override
    void removeIndexInternal(Comparable oldValue, Data indexKey) {
        removeFunctor.invoke(oldValue, indexKey);
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
                recordsWithNullValue = Collections.emptyMap();
            } else {
                recordsWithNullValue.clear();
            }
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
            Comparable paramFrom = from;
            Comparable paramTo = to;
            int trend = paramFrom.compareTo(paramTo);
            if (trend == 0) {
                Map<Data, QueryableEntry> records = recordMap.get(paramFrom);
                if (records != null) {
                    copyToMultiResultSet(results, records);
                }
                return results;
            }
            if (trend < 0) {
                Comparable oldFrom = paramFrom;
                paramFrom = to;
                paramTo = oldFrom;
            }
            for (Map.Entry<Comparable, Map<Data, QueryableEntry>> recordMapEntry : recordMap.entrySet()) {
                Comparable value = recordMapEntry.getKey();
                if (value.compareTo(paramFrom) <= 0 && value.compareTo(paramTo) >= 0) {
                    Map<Data, QueryableEntry> records = recordMapEntry.getValue();
                    if (records != null) {
                        copyToMultiResultSet(results, records);
                    }
                }
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
            for (Map.Entry<Comparable, Map<Data, QueryableEntry>> recordMapEntry : recordMap.entrySet()) {
                Comparable value = recordMapEntry.getKey();
                boolean valid;
                int result = searchedValue.compareTo(value);
                switch (comparisonType) {
                    case LESSER:
                        valid = result > 0;
                        break;
                    case LESSER_EQUAL:
                        valid = result >= 0;
                        break;
                    case GREATER:
                        valid = result < 0;
                        break;
                    case GREATER_EQUAL:
                        valid = result <= 0;
                        break;
                    case NOT_EQUAL:
                        valid = result != 0;
                        break;
                    default:
                        throw new IllegalStateException("Unrecognized comparisonType: " + comparisonType);
                }
                if (valid) {
                    Map<Data, QueryableEntry> records = recordMapEntry.getValue();
                    if (records != null) {
                        copyToMultiResultSet(results, records);
                    }
                }
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value instanceof IndexImpl.NullObject) {
                return toSingleResultSet(recordsWithNullValue);
            } else {
                return toSingleResultSet(recordMap.get(value));
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
                Map<Data, QueryableEntry> records;
                if (value instanceof IndexImpl.NullObject) {
                    records = recordsWithNullValue;
                } else {
                    records = recordMap.get(value);
                }
                if (records != null) {
                    copyToMultiResultSet(results, records);
                }
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    /**
     * Adds entry to the given index map without copying it.
     * Needs to be invoked in a thread-safe way.
     *
     * @see IndexCopyBehavior
     */
    private class AddFunctor implements IndexFunctor<Comparable, QueryableEntry> {
        @Override
        public void invoke(Comparable attribute, QueryableEntry entry) {
            if (attribute instanceof IndexImpl.NullObject) {
                recordsWithNullValue.put(entry.getKeyData(), entry);
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(attribute);
                if (records == null) {
                    records = new ConcurrentHashMap<Data, QueryableEntry>(1, LOAD_FACTOR, 1);
                    recordMap.put(attribute, records);
                }
                records.put(entry.getKeyData(), entry);
            }
        }
    }

    /**
     * Adds entry to the given index map copying it to secure exclusive access.
     * Needs to be invoked in a thread-safe way.
     *
     * @see IndexCopyBehavior
     */
    private class CopyOnWriteAddFunctor implements IndexFunctor<Comparable, QueryableEntry> {
        @Override
        public void invoke(Comparable attribute, QueryableEntry entry) {
            if (attribute instanceof IndexImpl.NullObject) {
                HashMap<Data, QueryableEntry> copy = new HashMap<Data, QueryableEntry>(recordsWithNullValue);
                copy.put(entry.getKeyData(), entry);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(attribute);
                if (records == null) {
                    records = Collections.emptyMap();
                }

                records = new HashMap<Data, QueryableEntry>(records);
                records.put(entry.getKeyData(), entry);

                recordMap.put(attribute, records);
            }
        }
    }

    /**
     * Removes entry from the given index map without copying it.
     * Needs to be invoked in a thread-safe way.
     *
     * @see IndexCopyBehavior
     */
    private class RemoveFunctor implements IndexFunctor<Comparable, Data> {
        @Override
        public void invoke(Comparable attribute, Data indexKey) {
            if (attribute instanceof IndexImpl.NullObject) {
                recordsWithNullValue.remove(indexKey);
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(attribute);
                if (records != null) {
                    records.remove(indexKey);
                    if (records.size() == 0) {
                        recordMap.remove(attribute);
                    }
                }
            }
        }
    }

    /**
     * Removes entry from the given index map copying it to secure exclusive access.
     * Needs to be invoked in a thread-safe way.
     *
     * @see IndexCopyBehavior
     */
    private class CopyOnWriteRemoveFunctor implements IndexFunctor<Comparable, Data> {
        @Override
        public void invoke(Comparable attribute, Data indexKey) {
            if (attribute instanceof IndexImpl.NullObject) {
                HashMap<Data, QueryableEntry> copy = new HashMap<Data, QueryableEntry>(recordsWithNullValue);
                copy.remove(indexKey);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(attribute);
                if (records != null) {
                    records = new HashMap<Data, QueryableEntry>(records);
                    records.remove(indexKey);

                    if (records.isEmpty()) {
                        recordMap.remove(attribute);
                    } else {
                        recordMap.put(attribute, records);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "UnsortedIndexStore{"
                + "recordMap=" + recordMap.size()
                + '}';
    }
}
