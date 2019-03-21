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
import com.hazelcast.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.MultiResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    interface CopyFunctor<A, B> {

        Map<A, B> invoke(Map<A, B> map);

    }

    interface IndexFunctor<A, B> {

        Object invoke(A param1, B param2);

    }

    private static class PassThroughFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            return map;
        }

    }

    private static class CopyInputFunctor implements CopyFunctor<Data, QueryableEntry> {

        @Override
        public Map<Data, QueryableEntry> invoke(Map<Data, QueryableEntry> map) {
            if (map != null && !map.isEmpty()) {
                return new HashMap<Data, QueryableEntry>(map);
            }
            return map;
        }

    }

}
