/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
    public static final Comparator<Data> DATA_COMPARATOR = new DataComparator();

    private final ConcurrentSkipListMap<Comparable, NavigableMap<Data, QueryableEntry>> recordMap =
        new ConcurrentSkipListMap<>(Comparables.COMPARATOR);

    private final IndexFunctor<Comparable, QueryableEntry> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    private volatile SortedMap<Data, QueryableEntry> recordsWithNullValue;

    public OrderedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn, true);
        assert copyOn != null;
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
            addFunctor = new CopyOnWriteAddFunctor();
            removeFunctor = new CopyOnWriteRemoveFunctor();
            recordsWithNullValue = new TreeMap<>(DATA_COMPARATOR);
        } else {
            addFunctor = new AddFunctor();
            removeFunctor = new RemoveFunctor();
            recordsWithNullValue = new ConcurrentSkipListMap<>(DATA_COMPARATOR);
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
        return new IteratorFromBatch(getSqlRecordIteratorBatch(descending));
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        return new IteratorFromBatch(getSqlRecordIteratorBatch(value));
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable searchedValue, boolean descending) {
        return new IteratorFromBatch(getSqlRecordIteratorBatch(comparison, searchedValue, descending));
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(
        Comparable from,
        boolean fromInclusive,
        Comparable to,
        boolean toInclusive,
        boolean descending
    ) {
        return new IteratorFromBatch(getSqlRecordIteratorBatch(from, fromInclusive, to, toInclusive, descending));
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
        if (value == NULL) {
            return Stream.of(new IndexKeyEntries(value, recordsWithNullValue.values().iterator())).iterator();
        } else {
            Map<Data, QueryableEntry> entries = recordMap.get(value);

            if (entries == null) {
                return Collections.emptyIterator();
            } else {
                return Stream.of(new IndexKeyEntries(value, entries.values().iterator())).iterator();
            }
        }
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
        Stream<IndexKeyEntries> nullStream = Stream.of(
                new IndexKeyEntries(null, recordsWithNullValue.values().iterator()));

        if (descending) {
            Stream<IndexKeyEntries> nonNullStream = recordMap.descendingMap().entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().descendingMap().values().iterator()));

            return Stream.concat(nonNullStream, nullStream).iterator();
        } else {
            Stream<IndexKeyEntries> nonNullStream = recordMap.entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values().iterator()));

            return Stream.concat(nullStream, nonNullStream).iterator();
        }
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            Comparison comparison,
            Comparable searchedValue,
            boolean descending
    ) {
        ConcurrentNavigableMap<Comparable, NavigableMap<Data, QueryableEntry>> navigableMap
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

        if (descending) {
            return navigableMap.entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().descendingMap().values().iterator()))
                    .iterator();
        } else {
            return navigableMap.entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values().iterator()))
                    .iterator();
        }

    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
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

            return Stream.of(new IndexKeyEntries(from, res.values().iterator())).iterator();
        } else if (order > 0) {
            return emptyIterator();
        }

        ConcurrentNavigableMap<Comparable, NavigableMap<Data, QueryableEntry>> navigableMap =
                descending ? recordMap.descendingMap() : recordMap;
        Comparable from0 = descending ? to : from;
        boolean fromInclusive0 = descending ? toInclusive : fromInclusive;
        Comparable to0 = descending ? from : to;
        boolean toInclusive0 = descending ? fromInclusive : toInclusive;

        if (descending) {
            return navigableMap.subMap(from0, fromInclusive0, to0, toInclusive0).entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().descendingMap().values().iterator()))
                    .iterator();
        } else {
            return navigableMap.subMap(from0, fromInclusive0, to0, toInclusive0).entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values().iterator()))
                    .iterator();
        }

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
            SortedMap<Comparable, NavigableMap<Data, QueryableEntry>> subMap;
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
            SortedMap<Comparable, NavigableMap<Data, QueryableEntry>> subMap =
                    recordMap.subMap(from, fromInclusive, to, toInclusive);
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
                NavigableMap<Data, QueryableEntry> records = recordMap.get(value);
                if (records == null) {
                    records = new ConcurrentSkipListMap<>(DATA_COMPARATOR);
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
                TreeMap<Data, QueryableEntry> copy = new TreeMap<>(recordsWithNullValue);
                oldValue = copy.put(entry.getKeyData(), entry);
                recordsWithNullValue = copy;
            } else {
                NavigableMap<Data, QueryableEntry> records = recordMap.get(value);
                if (records == null) {
                    records = new TreeMap<>(DATA_COMPARATOR);
                }

                records = new TreeMap<>(records);
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
                TreeMap<Data, QueryableEntry> copy = new TreeMap<>(recordsWithNullValue);
                oldValue = copy.remove(indexKey);
                recordsWithNullValue = copy;
            } else {
                NavigableMap<Data, QueryableEntry> records = recordMap.get(value);
                if (records != null) {
                    records = new TreeMap<>(records);
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

    private static final class IteratorFromBatch implements Iterator<QueryableEntry> {
        private final Iterator<IndexKeyEntries> iterator;
        private Iterator<QueryableEntry> indexKeyIterator;

        private IteratorFromBatch(@Nonnull Iterator<IndexKeyEntries> iterator) {
            this.iterator = iterator;
            this.indexKeyIterator = iterator.hasNext() ? iterator.next().getEntries() : null;
        }

        @Override
        public boolean hasNext() {
            if (indexKeyIterator == null) {
                return false;
            }
            if (indexKeyIterator.hasNext()) {
                return true;
            } else {
                while (iterator.hasNext()) {
                    indexKeyIterator = iterator.next().getEntries();
                    if (indexKeyIterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public QueryableEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return indexKeyIterator.next();
        }
    }

    private static class DataComparator implements Comparator<Data> {

        @Override
        public int compare(Data o1, Data o2) {
            byte[] thisBytes = o1.toByteArray();
            byte[] thatBytes = o2.toByteArray();
            int minLen = Math.min(thisBytes.length, thatBytes.length);
            for (int i = 0; i < minLen; i++) {
                int diff = thisBytes[i] - thatBytes[i];
                if (diff == 0) {
                    continue;
                }
                return diff;
            }
            return thisBytes.length - thatBytes.length;
        }
    }
}
