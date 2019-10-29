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

import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.internal.util.Clock;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

/**
 * Base class for concrete index store implementations
 */
public abstract class BaseIndexStore implements IndexStore {

    static final float LOAD_FACTOR = 0.75F;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private final CopyFunctor<Data, QueryableEntry> resultCopyFunctor;

    private boolean multiResultHasToDetectDuplicates;
    /**
     * {@code true} if this index store has at least one candidate entry
     * for expiration (idle or tll), otherwise {@code false}.
     * <p>
     * The field is updated on every update of the index.
     * <p>
     * The filed's access is guarded by {@link BaseIndexStore#lock}.
     */
    private boolean isIndexStoreExpirable;

    BaseIndexStore(IndexCopyBehavior copyOn) {
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE || copyOn == IndexCopyBehavior.NEVER) {
            resultCopyFunctor = new PassThroughFunctor();
        } else {
            resultCopyFunctor = new CopyInputFunctor();
        }
    }

    /**
     * Canonicalizes the given value for storing it in this index store.
     * <p>
     * The method is used by hash indexes to achieve the canonical
     * representation of mixed-type numeric values, so {@code equals} and {@code
     * hashCode} logic can work properly.
     * <p>
     * The main difference comparing to {@link IndexStore#canonicalizeQueryArgumentScalar}
     * is that this method is specifically designed to support the
     * canonicalization of persistent index values (think of map entry attribute
     * values), so a more suitable value representation may chosen.
     *
     * @param value the value to canonicalize.
     * @return the canonicalized value.
     */
    abstract Comparable canonicalizeScalarForStorage(Comparable value);

    /**
     * Associates the given value in this index store with the given record.
     * <p>
     * Despite the name the given value acts as a key into this index store. In
     * other words, it's a value of an attribute this index store is built for.
     *
     * @param value  the value of an attribute this index store is built for.
     * @param record the record to associate with the given value.
     * @return the record that was associated with the given value before the
     * operation, if there was any, {@code null} otherwise.
     */
    abstract Object insertInternal(Comparable value, QueryableEntry record);

    /**
     * Removes the association between the given value and a record identified
     * by the given record key.
     * <p>
     * Despite the name the given value acts as a key into this index store. In
     * other words, it's a value of an attribute this index store is built for.
     *
     * @param value     the value of an attribute this index store is built for.
     * @param recordKey the key of a record to dissociate from the given value.
     * @return the record that was associated with the given value before the
     * operation, if there was any, {@code null} otherwise.
     */
    abstract Object removeInternal(Comparable value, Data recordKey);

    void takeWriteLock() {
        writeLock.lock();
    }

    void releaseWriteLock() {
        writeLock.unlock();
    }

    void takeReadLock() {
        readLock.lock();
    }

    void releaseReadLock() {
        readLock.unlock();
    }

    final MultiResultSet createMultiResultSet() {
        return multiResultHasToDetectDuplicates ? new DuplicateDetectingMultiResult() : new FastMultiResultSet();
    }

    final void copyToMultiResultSet(MultiResultSet resultSet, Map<Data, QueryableEntry> records) {
        resultSet.addResultSet(resultCopyFunctor.invoke(records));
    }

    final Set<QueryableEntry> toSingleResultSet(Map<Data, QueryableEntry> records) {
        return new SingleResultSet(resultCopyFunctor.invoke(records));
    }

    @Override
    public final void insert(Object value, QueryableEntry record, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndInsertToIndex(value, record, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public final void update(Object oldValue, Object newValue, QueryableEntry entry, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            Data indexKey = entry.getKeyData();
            unwrapAndRemoveFromIndex(oldValue, indexKey, operationStats);
            unwrapAndInsertToIndex(newValue, entry, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public final void remove(Object value, Data indexKey, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndRemoveFromIndex(value, indexKey, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void destroy() {
        // nothing to destroy
    }

    @SuppressWarnings("unchecked")
    private void unwrapAndInsertToIndex(Object newValue, QueryableEntry record, IndexOperationStats operationStats) {
        if (newValue == NonTerminalJsonValue.INSTANCE) {
            return;
        }
        if (newValue instanceof MultiResult) {
            multiResultHasToDetectDuplicates = true;
            List<Object> results = ((MultiResult) newValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                Object oldValue = insertInternal(sanitizedValue, record);
                operationStats.onEntryAdded(oldValue, newValue);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(newValue);
            Object oldValue = insertInternal(sanitizedValue, record);
            operationStats.onEntryAdded(oldValue, newValue);
        }
    }

    @SuppressWarnings("unchecked")
    private void unwrapAndRemoveFromIndex(Object oldValue, Data indexKey, IndexOperationStats operationStats) {
        if (oldValue == NonTerminalJsonValue.INSTANCE) {
            return;
        }
        if (oldValue instanceof MultiResult) {
            List<Object> results = ((MultiResult) oldValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                Object removedValue = removeInternal(sanitizedValue, indexKey);
                operationStats.onEntryRemoved(removedValue);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(oldValue);
            Object removedValue = removeInternal(sanitizedValue, indexKey);
            operationStats.onEntryRemoved(removedValue);
        }
    }

    private Comparable sanitizeValue(Object input) {
        if (input instanceof CompositeValue) {
            CompositeValue compositeValue = (CompositeValue) input;
            Comparable[] components = compositeValue.getComponents();
            for (int i = 0; i < components.length; ++i) {
                components[i] = sanitizeScalar(components[i]);
            }
            return compositeValue;
        } else {
            return sanitizeScalar(input);
        }
    }

    private Comparable sanitizeScalar(Object input) {
        if (input == null || input instanceof Comparable) {
            Comparable value = (Comparable) input;
            if (value == null) {
                value = NULL;
            } else if (value.getClass().isEnum()) {
                value = TypeConverters.ENUM_CONVERTER.convert(value);
            }
            return canonicalizeScalarForStorage(value);
        } else {
            throw new IllegalArgumentException("It is not allowed to use a type that is not Comparable: " + input.getClass());
        }
    }

    void markIndexStoreExpirableIfNecessary(QueryableEntry record) {
        assert lock.isWriteLockedByCurrentThread();
        // StoreAdapter is not set in plenty of internal unit tests
        if (record.getStoreAdapter() != null) {
            isIndexStoreExpirable = record.getStoreAdapter().isExpirable();
        }
    }

    boolean isExpirable() {
        return isIndexStoreExpirable;
    }

    interface CopyFunctor<A, B> {

        Map<A, B> invoke(Map<A, B> map);

    }

    interface IndexFunctor<A, B> {

        Object invoke(A param1, B param2);

    }

    private class PassThroughFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            if (isExpirable()) {
                return new ExpirationAwareHashMapDelegate(map);
            }
            return map;
        }

    }

    private class CopyInputFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            if (map != null && !map.isEmpty()) {
                HashMap<Data, QueryableEntry> newMap = new HashMap<Data, QueryableEntry>(map);
                if (isExpirable()) {
                    return new ExpirationAwareHashMapDelegate(newMap);
                }
                return newMap;
            }
            return map;
        }

    }

    /**
     * This delegating Map updates the {@link Record}'s access time on every
     * {@link QueryableEntry} retrieved through the {@link Map#entrySet()},
     * {@link Map#get} or {@link Map#values()}.
     *
     */
    private static class ExpirationAwareHashMapDelegate implements Map<Data, QueryableEntry> {

        private final Map<Data, QueryableEntry> delegateMap;

        ExpirationAwareHashMapDelegate(Map<Data, QueryableEntry> map) {
            this.delegateMap = map;
        }

        @Override
        public int size() {
            return delegateMap.size();
        }

        @Override
        public boolean isEmpty() {
            return delegateMap.isEmpty();
        }

        @Override
        public boolean containsKey(Object o) {
            return delegateMap.containsKey(o);
        }

        @Override
        public boolean containsValue(Object o) {
            return delegateMap.containsValue(o);
        }

        @Override
        public QueryableEntry put(Data data, QueryableEntry queryableEntry) {
            return delegateMap.put(data, queryableEntry);
        }

        @Override
        public QueryableEntry remove(Object o) {
            return delegateMap.remove(o);
        }

        @Override
        public void putAll(Map<? extends Data, ? extends QueryableEntry> map) {
            delegateMap.putAll(map);
        }

        @Override
        public void clear() {
            delegateMap.clear();
        }

        @Override
        public Set<Data> keySet() {
            return delegateMap.keySet();
        }

        @Override
        public QueryableEntry get(Object o) {
            QueryableEntry queryableEntry = delegateMap.get(o);
            if (queryableEntry != null) {
                long now = Clock.currentTimeMillis();
                queryableEntry.getRecord().onAccessSafe(now);
            }
            return queryableEntry;
        }

        @Override
        public Collection<QueryableEntry> values() {
            long now = Clock.currentTimeMillis();
            return new ExpirationAwareSet<QueryableEntry>(delegateMap.values(),
                    queryableEntry -> queryableEntry.getRecord().onAccessSafe(now));
        }

        @Override
        public Set<Entry<Data, QueryableEntry>> entrySet() {
            long now = Clock.currentTimeMillis();
            return new ExpirationAwareSet<Entry<Data, QueryableEntry>>(delegateMap.entrySet(),
                    entry -> entry.getValue().getRecord().onAccessSafe(now));
        }

        private static class ExpirationAwareSet<V> extends AbstractSet<V> {

            private final Collection<V> delegateCollection;
            private final Consumer<V> recordUpdater;

            ExpirationAwareSet(Collection<V> delegateCollection, Consumer<V> recordUpdater) {
                this.delegateCollection = delegateCollection;
                this.recordUpdater = recordUpdater;
            }

            @Override
            public int size() {
                return delegateCollection.size();
            }

            @Override
            public Iterator<V> iterator() {
                return new ExpirationAwareIterator(delegateCollection.iterator());
            }

            public boolean add(V v) {
                return delegateCollection.add(v);
            }

            @Override
            public boolean remove(Object o) {
                return delegateCollection.remove(o);
            }

            @Override
            public void clear() {
                delegateCollection.clear();
            }

            private class ExpirationAwareIterator implements Iterator<V> {

                private final Iterator<V> delegateIterator;

                ExpirationAwareIterator(Iterator<V> iterator) {
                    this.delegateIterator = iterator;
                }

                @Override
                public boolean hasNext() {
                    return delegateIterator.hasNext();
                }

                @Override
                public V next() {
                    V next = delegateIterator.next();
                    if (next != null) {
                        recordUpdater.accept(next);
                    }
                    return next;
                }

                @Override
                public void remove() {
                    delegateIterator.remove();
                }
            }
        }
    }
}
