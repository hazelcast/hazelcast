/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static java.util.Collections.emptySet;

/**
 * Store indexes rankly.
 */
public class OrderedIndexStore extends BaseIndexStore {

    private final ConcurrentSkipListMap<Comparable, Map<Data, QueryableEntryImpl>> recordMap =
            new ConcurrentSkipListMap<Comparable, Map<Data, QueryableEntryImpl>>(Comparables.COMPARATOR);

    private final IndexFunctor<Comparable, QueryableEntryImpl> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    private volatile Map<Data, QueryableEntryImpl> recordsWithNullValue;

    public OrderedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn);
        assert copyOn != null;
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
            addFunctor = new CopyOnWriteAddFunctor();
            removeFunctor = new CopyOnWriteRemoveFunctor();
            recordsWithNullValue = Collections.emptyMap();
        } else {
            addFunctor = new AddFunctor();
            removeFunctor = new RemoveFunctor();
            recordsWithNullValue = new ConcurrentHashMap<Data, QueryableEntryImpl>();
        }
    }

    @Override
    Object insertInternal(Comparable value, QueryableEntryImpl record) {
        return addFunctor.invoke(value, record);
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        return removeFunctor.invoke(value, recordKey);
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // We still need to canonicalize query arguments for ordered indexes to
        // support InPredicate queries.
        return Comparables.canonicalizeForHashLookup(value);
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        // Returning the original value since ordered indexes are not supporting
        // hash lookups on their stored values, so there is no need in providing
        // canonical representations.
        return value;
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
    public Set<QueryableEntryImpl> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value == NULL) {
                return toSingleResultSet(recordsWithNullValue);
            } else {
                return toSingleResultSet(recordMap.get(value));
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntryImpl> getRecords(Set<Comparable> values) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            for (Comparable value : values) {
                Map<Data, QueryableEntryImpl> records;
                if (value == NULL) {
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

    @Override
    public Set<QueryableEntryImpl> getRecords(Comparison comparison, Comparable searchedValue) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            SortedMap<Comparable, Map<Data, QueryableEntryImpl>> subMap;
            switch (comparison) {
                case LESS:
                    subMap = recordMap.headMap(searchedValue, false);
                    break;
                case LESS_OR_EQUAL:
                    subMap = recordMap.headMap(searchedValue, true);
                    break;
                case GREATER:
                    subMap = recordMap.tailMap(searchedValue, false);
                    break;
                case GREATER_OR_EQUAL:
                    subMap = recordMap.tailMap(searchedValue, true);
                    break;
                case NOT_EQUAL:
                    for (Map.Entry<Comparable, Map<Data, QueryableEntryImpl>> entry : recordMap.entrySet()) {
                        if (Comparables.compare(searchedValue, entry.getKey()) != 0) {
                            copyToMultiResultSet(results, entry.getValue());
                        }
                    }
                    return results;
                default:
                    throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
            }
            for (Map<Data, QueryableEntryImpl> value : subMap.values()) {
                copyToMultiResultSet(results, value);
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntryImpl> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        takeReadLock();
        try {
            int order = Comparables.compare(from, to);
            if (order == 0) {
                if (!fromInclusive || !toInclusive) {
                    return emptySet();
                }
                return toSingleResultSet(recordMap.get(from));
            } else if (order > 0) {
                return emptySet();
            }

            MultiResultSet results = createMultiResultSet();
            SortedMap<Comparable, Map<Data, QueryableEntryImpl>> subMap = recordMap.subMap(from, fromInclusive, to, toInclusive);
            for (Map<Data, QueryableEntryImpl> value : subMap.values()) {
                copyToMultiResultSet(results, value);
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
    private class AddFunctor implements IndexFunctor<Comparable, QueryableEntryImpl> {

        @Override
        public Object invoke(Comparable value, QueryableEntryImpl entry) {
            if (value == NULL) {
                return recordsWithNullValue.put(entry.getKeyData(), entry);
            } else {
                Map<Data, QueryableEntryImpl> records = recordMap.get(value);
                if (records == null) {
                    records = new ConcurrentHashMap<Data, QueryableEntryImpl>(1, LOAD_FACTOR, 1);
                    recordMap.put(value, records);
                }
                return records.put(entry.getKeyData(), entry);
            }
        }

    }

    /**
     * Adds entry to the given index map copying it to secure exclusive access.
     * Needs to be invoked in a thread-safe way.
     *
     * @see IndexCopyBehavior
     */
    private class CopyOnWriteAddFunctor implements IndexFunctor<Comparable, QueryableEntryImpl> {

        @Override
        public Object invoke(Comparable value, QueryableEntryImpl entry) {
            Object oldValue;
            if (value == NULL) {
                HashMap<Data, QueryableEntryImpl> copy = new HashMap<Data, QueryableEntryImpl>(recordsWithNullValue);
                oldValue = copy.put(entry.getKeyData(), entry);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntryImpl> records = recordMap.get(value);
                if (records == null) {
                    records = Collections.emptyMap();
                }

                records = new HashMap<Data, QueryableEntryImpl>(records);
                oldValue = records.put(entry.getKeyData(), entry);

                recordMap.put(value, records);
            }
            return oldValue;
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
        public Object invoke(Comparable value, Data indexKey) {
            Object oldValue;
            if (value == NULL) {
                oldValue = recordsWithNullValue.remove(indexKey);
            } else {
                Map<Data, QueryableEntryImpl> records = recordMap.get(value);
                if (records != null) {
                    oldValue = records.remove(indexKey);
                    if (records.size() == 0) {
                        recordMap.remove(value);
                    }
                } else {
                    oldValue = null;
                }
            }

            return oldValue;
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
        public Object invoke(Comparable value, Data indexKey) {
            Object oldValue;
            if (value == NULL) {
                HashMap<Data, QueryableEntryImpl> copy = new HashMap<Data, QueryableEntryImpl>(recordsWithNullValue);
                oldValue = copy.remove(indexKey);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntryImpl> records = recordMap.get(value);
                if (records != null) {
                    records = new HashMap<Data, QueryableEntryImpl>(records);
                    oldValue = records.remove(indexKey);

                    if (records.isEmpty()) {
                        recordMap.remove(value);
                    } else {
                        recordMap.put(value, records);
                    }
                } else {
                    oldValue = null;
                }
            }

            return oldValue;
        }

    }

}
