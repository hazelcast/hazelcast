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
import com.hazelcast.internal.util.FlatCompositeIterator;
import com.hazelcast.query.Predicate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

/**
 * Store indexes out of turn.
 */
@SuppressWarnings("rawtypes")
public class UnorderedIndexStore extends BaseSingleValueIndexStore {

    private final ConcurrentMap<Comparable, Map<Data, QueryableEntry>> recordMap = new ConcurrentHashMap<>();
    private final IndexFunctor<Comparable, QueryableEntry> addFunctor;
    private final IndexFunctor<Comparable, Data> removeFunctor;

    private volatile Map<Data, QueryableEntry> recordsWithNullValue;

    public UnorderedIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn, true);
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
        // Using a storage representation for arguments here to save on
        // conversions later.
        return canonicalizeScalarForStorage(value);
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        // Assuming on-heap overhead of 12 bytes for the object header and
        // allocation granularity by modulo 8, there is no point in trying to
        // represent a value in less than 4 bytes.

        if (!(value instanceof Number)) {
            return value;
        }

        Class clazz = value.getClass();
        Number number = (Number) value;

        if (clazz == Double.class) {
            double doubleValue = number.doubleValue();

            long longValue = number.longValue();
            if (Numbers.equalDoubles(doubleValue, (double) longValue)) {
                return canonicalizeLongRepresentable(longValue);
            }

            float floatValue = number.floatValue();
            if (doubleValue == (double) floatValue) {
                return floatValue;
            }
        } else if (clazz == Float.class) {
            float floatValue = number.floatValue();

            long longValue = number.longValue();
            if (Numbers.equalFloats(floatValue, (float) longValue)) {
                return canonicalizeLongRepresentable(longValue);
            }
        } else if (Numbers.isLongRepresentable(clazz)) {
            return canonicalizeLongRepresentable(number.longValue());
        }

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
        Iterator<QueryableEntry> iterator = new IndexEntryFlatteningIterator(recordMap.values().iterator());
        Iterator<QueryableEntry> nullIterator = recordsWithNullValue.values().iterator();

        return new FlatCompositeIterator<>(Arrays.asList(nullIterator, iterator).iterator());
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        if (value == NULL) {
            return recordsWithNullValue.values().iterator();
        } else {
            Map<Data, QueryableEntry> res = recordMap.get(canonicalize(value));

            if (res == null) {
                return Collections.emptyIterator();
            }

            return res.values().iterator();
        }
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparable value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(Comparison comparison, Comparable value, boolean descending) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive,
            boolean descending
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value == NULL) {
                return toSingleResultSet(recordsWithNullValue);
            } else {
                return toSingleResultSet(recordMap.get(canonicalize(value)));
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
                    // value is already canonicalized by the associated index
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
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            for (Map.Entry<Comparable, Map<Data, QueryableEntry>> recordMapEntry : recordMap.entrySet()) {
                Comparable indexedValue = recordMapEntry.getKey();
                boolean valid;
                int result = Comparables.compare(value, indexedValue);
                switch (comparison) {
                    case LESS:
                        valid = result > 0;
                        break;
                    case LESS_OR_EQUAL:
                        valid = result >= 0;
                        break;
                    case GREATER:
                        valid = result < 0;
                        break;
                    case GREATER_OR_EQUAL:
                        valid = result <= 0;
                        break;
                    default:
                        throw new IllegalStateException("Unrecognized comparison: " + comparison);
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

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            if (Comparables.compare(from, to) == 0) {
                if (!fromInclusive || !toInclusive) {
                    return results;
                }

                Map<Data, QueryableEntry> records = recordMap.get(canonicalize(from));
                if (records != null) {
                    copyToMultiResultSet(results, records);
                }
                return results;
            }

            int fromBound = fromInclusive ? 0 : +1;
            int toBound = toInclusive ? 0 : -1;
            for (Map.Entry<Comparable, Map<Data, QueryableEntry>> recordMapEntry : recordMap.entrySet()) {
                Comparable value = recordMapEntry.getKey();
                if (Comparables.compare(value, from) >= fromBound && Comparables.compare(value, to) <= toBound) {
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
                    records = new HashMap<>();
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

    private Comparable canonicalize(Comparable value) {
        if (value instanceof CompositeValue) {
            Comparable[] components = ((CompositeValue) value).getComponents();
            for (int i = 0; i < components.length; ++i) {
                components[i] = canonicalizeScalarForStorage(components[i]);
            }
            return value;
        } else {
            return canonicalizeScalarForStorage(value);
        }
    }

    private static Comparable canonicalizeLongRepresentable(long value) {
        if (value == (long) (int) value) {
            return (int) value;
        } else {
            return value;
        }
    }

}
