/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Arrays;
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
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptySet;

/**
 * Store indexes rankly.
 */
@SuppressWarnings("rawtypes")
public class OrderedIndexStore extends BaseSingleValueIndexStore {
    public static final Comparator<Data> DATA_COMPARATOR = new DataComparator();
    public static final Comparator<Data> DATA_COMPARATOR_REVERSED = new DataComparator().reversed();

    public static final Comparator<Comparable> SPECIAL_AWARE_COMPARATOR = (left, right) -> {
        // compare to explicit instances of special Comparables to avoid infinite loop
        // NEGATIVE_INFINITY should not be used in the index or queries
        // - the same result can be achieved by inclusive NULL or null.
        if (right == NULL) {
            return -NULL.compareTo(left);
        } else if (right == POSITIVE_INFINITY) {
            return -POSITIVE_INFINITY.compareTo(left);
        } else if (left == null && right == null) {
            return 0;
        } else if (left != null && right != null) {
            return Comparables.compare(left, right);
        } else if (left == null) {
            return -1;
        } else {
            return 1;
        }
    };

    private final ConcurrentSkipListMap<Comparable, NavigableMap<Data, QueryableEntry>> recordMap =
        new ConcurrentSkipListMap<>(SPECIAL_AWARE_COMPARATOR);

    private final IndexFunctor<Comparable, QueryableEntry> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    public OrderedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn, true);
        assert copyOn != null;
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE) {
            addFunctor = new CopyOnWriteAddFunctor();
            removeFunctor = new CopyOnWriteRemoveFunctor();
        } else {
            addFunctor = new AddFunctor();
            removeFunctor = new RemoveFunctor();
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
    public Iterator<QueryableEntry> getSqlRecordIterator(@Nonnull Comparable value) {
        return new IteratorFromBatch(getSqlRecordIteratorBatch(value, false));
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
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value, boolean descending) {
        NavigableMap<Data, QueryableEntry> entries = recordMap.get(value);

        if (entries == null) {
            return Collections.emptyIterator();
        } else {
            return Stream.of(new IndexKeyEntries(value,
                    (descending ? entries.descendingMap() : entries).values().iterator())).iterator();
        }
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
        if (descending) {
            return recordMap.descendingMap().entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().descendingMap().values().iterator()))
                    .iterator();
        } else {
            return recordMap.entrySet()
                    .stream()
                    .map((Entry<Comparable, NavigableMap<Data, QueryableEntry>> es) ->
                            new IndexKeyEntries(es.getKey(), es.getValue().values().iterator()))
                    .iterator();
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
                return getSqlRecordIteratorBatch(NULL, false, searchedValue, false, descending);
            case LESS_OR_EQUAL:
                return getSqlRecordIteratorBatch(NULL, false, searchedValue, true, descending);
            case GREATER:
                return getSqlRecordIteratorBatch(searchedValue, false, POSITIVE_INFINITY, true, descending);
            case GREATER_OR_EQUAL:
                return getSqlRecordIteratorBatch(searchedValue, true, POSITIVE_INFINITY, true, descending);
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
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
        int order = SPECIAL_AWARE_COMPARATOR.compare(from, to);

        if (order == 0) {
            if (!fromInclusive || !toInclusive) {
                return emptyIterator();
            }
            return getSqlRecordIteratorBatch(from, descending);
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
            return toSingleResultSet(recordMap.get(value));
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
                records = recordMap.get(value);
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
        switch (comparison) {
            case LESS:
                return getRecords(NULL, false, searchedValue, false);
            case LESS_OR_EQUAL:
                return getRecords(NULL, false, searchedValue, true);
            case GREATER:
                return getRecords(searchedValue, false, POSITIVE_INFINITY, true);
            case GREATER_OR_EQUAL:
                return getRecords(searchedValue, true, POSITIVE_INFINITY, true);
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        takeReadLock();
        try {
            int order = SPECIAL_AWARE_COMPARATOR.compare(from, to);
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
            return recordMap.computeIfAbsent(value, x -> new ConcurrentSkipListMap<>(DATA_COMPARATOR)).put(entry.getKeyData(),
                    entry);
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
            NavigableMap<Data, QueryableEntry> records = recordMap.get(value);
            if (records == null) {
                records = new TreeMap<>(DATA_COMPARATOR);
            }

            records = new TreeMap<>(records);
            oldValue = records.put(entry.getKeyData(), entry);

            recordMap.put(value, records);
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
            Map<Data, QueryableEntry> records = recordMap.get(value);
            if (records != null) {
                oldValue = records.remove(indexKey);
                if (records.isEmpty()) {
                    recordMap.remove(value);
                }
            } else {
                oldValue = null;
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
            return Arrays.compare(thisBytes, thatBytes);
        }
    }
}
