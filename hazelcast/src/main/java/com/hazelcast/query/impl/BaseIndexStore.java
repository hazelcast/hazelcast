/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.record.Record;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final CopyFunctor<Data, QueryableEntry> resultCopyFunctor;

    /**
     * {@code true} if this index store has at least one candidate entry
     * for expiration (idle or tll), otherwise {@code false}.
     * <p>
     * The field is updated on every update of the index.
     */
    private volatile boolean isIndexStoreExpirable;

    BaseIndexStore(IndexCopyBehavior copyOn, boolean enableGlobalLock) {
        if (copyOn == IndexCopyBehavior.COPY_ON_WRITE || copyOn == IndexCopyBehavior.NEVER) {
            resultCopyFunctor = new PassThroughFunctor();
        } else {
            resultCopyFunctor = new CopyInputFunctor();
        }
        lock = enableGlobalLock ? new ReentrantReadWriteLock() : null;
        readLock = enableGlobalLock ? lock.readLock() : null;
        writeLock = enableGlobalLock ? lock.writeLock() : null;
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

    void takeWriteLock() {
        if (lock != null) {
            writeLock.lock();
        }
    }

    void releaseWriteLock() {
        if (lock != null) {
            writeLock.unlock();
        }
    }

    void takeReadLock() {
        if (lock != null) {
            readLock.lock();
        }
    }

    void releaseReadLock() {
        if (lock != null) {
            readLock.unlock();
        }
    }

    final void copyToMultiResultSet(MultiResultSet resultSet, Map<Data, QueryableEntry> records) {
        resultSet.addResultSet(resultCopyFunctor.invoke(records));
    }

    final Set<QueryableEntry> toSingleResultSet(Map<Data, QueryableEntry> records) {
        return new SingleResultSet(resultCopyFunctor.invoke(records));
    }

    @Override
    public void destroy() {
        // nothing to destroy
    }

    Comparable sanitizeValue(Object input) {
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
            if (map != null && isExpirable()) {
                return new ExpirationAwareHashMapDelegate(map);
            }
            return map;
        }

    }

    private class CopyInputFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            if (map != null && !map.isEmpty()) {
                HashMap<Data, QueryableEntry> newMap = new HashMap<>(map);
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
     */
    protected static final class ExpirationAwareHashMapDelegate implements Map<Data, QueryableEntry> {

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
            return new ExpirationAwareSet<>(delegateMap.values(), queryableEntry -> queryableEntry.getRecord().onAccessSafe(now));
        }

        @Override
        public Set<Entry<Data, QueryableEntry>> entrySet() {
            long now = Clock.currentTimeMillis();
            return new ExpirationAwareSet<>(delegateMap.entrySet(), entry -> entry.getValue().getRecord().onAccessSafe(now));
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
