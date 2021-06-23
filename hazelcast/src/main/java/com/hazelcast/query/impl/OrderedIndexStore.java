/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.FlatCompositeIterator;
import com.hazelcast.query.Predicate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptySet;

/**
 * Store indexes rankly.
 */
@SuppressWarnings("rawtypes")
public class OrderedIndexStore extends BaseSingleValueIndexStore {

    private final ConcurrentSkipListMap<Comparable, Map<Data, QueryableEntry>> recordMap =
        new ConcurrentSkipListMap<>(Comparables.COMPARATOR);

    private final IndexFunctor<Comparable, QueryableEntry> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    private volatile Map<Data, QueryableEntry> recordsWithNullValue;

    public OrderedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn, true);
        assert copyOn != null;
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
            addFunctor = new CopyOnWriteAddFunctor();
            removeFunctor = new CopyOnWriteRemoveFunctor();
            recordsWithNullValue = Collections.emptyMap();
        } else {
            addFunctor = new AddFunctor();
            removeFunctor = new RemoveFunctor();
            recordsWithNullValue = new ConcurrentHashMap<>();
        }
    }

    @Override
    Object insertInternal(Comparable value, QueryableEntry record) {
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
    public boolean isEvaluateOnly() {
        return false;
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return false;
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(boolean descending) {
        if (descending) {
            Iterator<QueryableEntry> iterator = new IndexEntryFlatteningIterator(recordMap.descendingMap().values().iterator());
            Iterator<QueryableEntry> nullIterator = recordsWithNullValue.values().iterator();

            return new FlatCompositeIterator<>(Arrays.asList(iterator, nullIterator).iterator());

        } else {
            Iterator<QueryableEntry> iterator = new IndexEntryFlatteningIterator(recordMap.values().iterator());
            Iterator<QueryableEntry> nullIterator = recordsWithNullValue.values().iterator();

            return new FlatCompositeIterator<>(Arrays.asList(nullIterator, iterator).iterator());
        }
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        if (value == NULL) {
            return recordsWithNullValue.values().iterator();
        } else {
            Map<Data, QueryableEntry> entries = recordMap.get(value);

            if (entries == null) {
                return Collections.emptyIterator();
            } else {
                return entries.values().iterator();
            }
        }
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable searchedValue, boolean descending) {
        Iterator<Map<Data, QueryableEntry>> iterator;

        ConcurrentNavigableMap navigableMap = descending ? recordMap.descendingMap() : recordMap;
        switch (comparison) {
            case LESS:
                if (descending) {
                    iterator = navigableMap.tailMap(searchedValue, false).values().iterator();
                } else {
                    iterator = navigableMap.headMap(searchedValue, false).values().iterator();
                }
                break;
            case LESS_OR_EQUAL:
                if (descending) {
                    iterator = navigableMap.tailMap(searchedValue, true).values().iterator();
                } else {
                    iterator = navigableMap.headMap(searchedValue, true).values().iterator();
                }
                break;
            case GREATER:
                if (descending) {
                    iterator = navigableMap.headMap(searchedValue, false).values().iterator();
                } else {
                    iterator = navigableMap.tailMap(searchedValue, false).values().iterator();
                }
                break;
            case GREATER_OR_EQUAL:
                if (descending) {
                    iterator = navigableMap.headMap(searchedValue, true).values().iterator();
                } else {
                    iterator = navigableMap.tailMap(searchedValue, true).values().iterator();
                }
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }

        return new IndexEntryFlatteningIterator(iterator);
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public Iterator<QueryableEntry> getSqlRecordIterator(
        Comparable from,
        boolean fromInclusive,
        Comparable to,
        boolean toInclusive,
        boolean descending
    ) {
        int order = Comparables.compare(from, to);

        if (order == 0) {
            if (!fromInclusive || !toInclusive) {
                return emptyIterator();
            }

            Map<Data, QueryableEntry> res = recordMap.get(from);

            if (res == null) {
                return emptyIterator();
            }

            return res.values().iterator();
        } else if (order > 0) {
            return emptyIterator();
        }

        ConcurrentNavigableMap navigableMap = descending ? recordMap.descendingMap() : recordMap;
        Comparable from0 = descending ? to : from;
        boolean fromInclusive0 = descending ? toInclusive : fromInclusive;
        Comparable to0 = descending ? from : to;
        boolean toInclusive0 = descending ? fromInclusive : toInclusive;
        return new IndexEntryFlatteningIterator(
            navigableMap.subMap(from0, fromInclusive0, to0, toInclusive0).values().iterator());
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
        if (value == NULL) {
            return Stream.of(new IndexKeyEntries(value, recordsWithNullValue.values())).iterator();
        } else {
            Map<Data, QueryableEntry> entries = recordMap.get(value);

            if (entries == null) {
                return Collections.emptyIterator();
            } else {
                return Stream.of(new IndexKeyEntries(value, entries.values())).iterator();
            }
        }
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
        Stream<IndexKeyEntries> nullStream = Stream.of(new IndexKeyEntries(null, recordsWithNullValue.values()));

        if (descending) {
            Stream<IndexKeyEntries> nonNullStream = recordMap.descendingMap().entrySet()
                    .stream()
                    .map((Entry<Comparable, Map<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values()));

            return Stream.concat(nonNullStream, nullStream).iterator();
        } else {
            Stream<IndexKeyEntries> nonNullStream = recordMap.entrySet()
                    .stream()
                    .map((Entry<Comparable, Map<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values()));

            return Stream.concat(nullStream, nonNullStream).iterator();
        }
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            Comparison comparison,
            Comparable searchedValue,
            boolean descending
    ) {
        ConcurrentNavigableMap<Comparable, Map<Data, QueryableEntry>> navigableMap
                = descending ? recordMap.descendingMap() : recordMap;
        switch (comparison) {
            case LESS:
                if (descending) {
                    navigableMap = navigableMap.tailMap(searchedValue, false);
                } else {
                    navigableMap = navigableMap.headMap(searchedValue, false);
                }
                break;
            case LESS_OR_EQUAL:
                if (descending) {
                    navigableMap = navigableMap.tailMap(searchedValue, true);
                } else {
                    navigableMap = navigableMap.headMap(searchedValue, true);
                }
                break;
            case GREATER:
                if (descending) {
                    navigableMap = navigableMap.headMap(searchedValue, false);
                } else {
                    navigableMap = navigableMap.tailMap(searchedValue, false);
                }
                break;
            case GREATER_OR_EQUAL:
                if (descending) {
                    navigableMap = navigableMap.headMap(searchedValue, true);
                } else {
                    navigableMap = navigableMap.tailMap(searchedValue, true);
                }
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }

        return navigableMap.entrySet()
                .stream()
                .map((Entry<Comparable, Map<Data, QueryableEntry>> es) ->
                        new IndexKeyEntries(es.getKey(), es.getValue().values()))
                .iterator();
    }

    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
    ) {
        int order = Comparables.compare(from, to);

        if (order == 0) {
            if (!fromInclusive || !toInclusive) {
                return emptyIterator();
            }

            Map<Data, QueryableEntry> res = recordMap.get(from);

            if (res == null) {
                return emptyIterator();
            }

            return Stream.of(new IndexKeyEntries(from, res.values())).iterator();
        } else if (order > 0) {
            return emptyIterator();
        }

        ConcurrentNavigableMap<Comparable, Map<Data, QueryableEntry>> navigableMap =
                descending ? recordMap.descendingMap() : recordMap;
        Comparable from0 = descending ? to : from;
        boolean fromInclusive0 = descending ? toInclusive : fromInclusive;
        Comparable to0 = descending ? from : to;
        boolean toInclusive0 = descending ? fromInclusive : toInclusive;

        return navigableMap.subMap(from0, fromInclusive0, to0, toInclusive0).entrySet()
                .stream()
                .map((Entry<Comparable, Map<Data, QueryableEntry>> es) ->
                        new IndexKeyEntries(es.getKey(), es.getValue().values()))
                .iterator();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
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
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            for (Comparable value : values) {
                Map<Data, QueryableEntry> records;
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
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable searchedValue) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            SortedMap<Comparable, Map<Data, QueryableEntry>> subMap;
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
                default:
                    throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
            }
            for (Map<Data, QueryableEntry> value : subMap.values()) {
                copyToMultiResultSet(results, value);
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
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
            SortedMap<Comparable, Map<Data, QueryableEntry>> subMap = recordMap.subMap(from, fromInclusive, to, toInclusive);
            for (Map<Data, QueryableEntry> value : subMap.values()) {
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
    private class AddFunctor implements IndexFunctor<Comparable, QueryableEntry> {

        @Override
        public Object invoke(Comparable value, QueryableEntry entry) {
            if (value == NULL) {
                return recordsWithNullValue.put(entry.getKeyData(), entry);
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(value);
                if (records == null) {
                    records = new ConcurrentHashMap<>(1, LOAD_FACTOR, 1);
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
    private class CopyOnWriteAddFunctor implements IndexFunctor<Comparable, QueryableEntry> {

        @Override
        public Object invoke(Comparable value, QueryableEntry entry) {
            Object oldValue;
            if (value == NULL) {
                HashMap<Data, QueryableEntry> copy = new HashMap<>(recordsWithNullValue);
                oldValue = copy.put(entry.getKeyData(), entry);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(value);
                if (records == null) {
                    records = Collections.emptyMap();
                }

                records = new HashMap<>(records);
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
                Map<Data, QueryableEntry> records = recordMap.get(value);
                if (records != null) {
                    oldValue = records.remove(indexKey);
                    if (records.isEmpty()) {
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
                HashMap<Data, QueryableEntry> copy = new HashMap<>(recordsWithNullValue);
                oldValue = copy.remove(indexKey);
                recordsWithNullValue = copy;
            } else {
                Map<Data, QueryableEntry> records = recordMap.get(value);
                if (records != null) {
                    records = new HashMap<>(records);
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
