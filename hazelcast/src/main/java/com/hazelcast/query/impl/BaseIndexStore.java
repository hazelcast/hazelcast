/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.MultiResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
     * Associates the given value in this index store with the given record.
     * <p>
     * Despite the name the given value acts as a key into this index store. In
     * other words, it's a value of an attribute this index store is built for.
     *
     * @param newValue the value of an attribute this index store is built for.
     * @param record   the record to associate with the given value.
     * @return the record that was associated with the given value before the
     * operation, if there was any, {@code null} otherwise.
     */
    abstract Object newIndexInternal(Comparable newValue, QueryableEntry record);

    /**
     * Removes the association between the given value and a record identified
     * by the given record key.
     * <p>
     * Despite the name the given value acts as a key into this index store. In
     * other words, it's a value of an attribute this index store is built for.
     *
     * @param oldValue  the value of an attribute this index store is built for.
     * @param recordKey the key of a record to dissociate from the given value.
     * @return the record that was associated with the given value before the
     * operation, if there was any, {@code null} otherwise.
     */
    abstract Object removeIndexInternal(Comparable oldValue, Data recordKey);

    @Override
    public final void newIndex(Object newValue, QueryableEntry record, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndAddToIndex(newValue, record, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    private void unwrapAndAddToIndex(Object newValue, QueryableEntry record, IndexOperationStats operationStats) {
        if (newValue instanceof MultiResult) {
            multiResultHasToDetectDuplicates = true;
            List<Object> results = ((MultiResult) newValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                Object oldValue = newIndexInternal(sanitizedValue, record);
                operationStats.onEntryAdded(oldValue, newValue);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(newValue);
            Object oldValue = newIndexInternal(sanitizedValue, record);
            operationStats.onEntryAdded(oldValue, newValue);
        }
    }

    @Override
    public final void removeIndex(Object oldValue, Data indexKey, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndRemoveFromIndex(oldValue, indexKey, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    private void unwrapAndRemoveFromIndex(Object oldValue, Data indexKey, IndexOperationStats operationStats) {
        if (oldValue instanceof MultiResult) {
            List<Object> results = ((MultiResult) oldValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                Object removedValue = removeIndexInternal(sanitizedValue, indexKey);
                operationStats.onEntryRemoved(removedValue);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(oldValue);
            Object removedValue = removeIndexInternal(sanitizedValue, indexKey);
            operationStats.onEntryRemoved(removedValue);
        }
    }

    @Override
    public final void updateIndex(Object oldValue, Object newValue, QueryableEntry entry,
                                  IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            Data indexKey = entry.getKeyData();
            unwrapAndRemoveFromIndex(oldValue, indexKey, operationStats);
            unwrapAndAddToIndex(newValue, entry, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void destroy() {
        // NOOP
    }

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

    private Comparable sanitizeValue(Object input) {
        if (input == null || input instanceof Comparable) {
            Comparable value = (Comparable) input;
            if (value == null) {
                value = IndexImpl.NULL;
            } else if (value.getClass().isEnum()) {
                value = TypeConverters.ENUM_CONVERTER.convert(value);
            }
            return value;
        } else {
            throw new IllegalArgumentException("It is not allowed to use a type that is not Comparable: "
                    + input.getClass());
        }

    }

    final MultiResultSet createMultiResultSet() {
        return multiResultHasToDetectDuplicates ? new DuplicateDetectingMultiResult() : new FastMultiResultSet();
    }

    interface CopyFunctor<A, B> {
        Map<A, B> invoke(Map<A, B> map);
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

    final void copyToMultiResultSet(MultiResultSet resultSet, Map<Data, QueryableEntry> records) {
        resultSet.addResultSet(resultCopyFunctor.invoke(records));
    }

    final Set<QueryableEntry> toSingleResultSet(Map<Data, QueryableEntry> records) {
        return new SingleResultSet(resultCopyFunctor.invoke(records));
    }

    interface IndexFunctor<A, B> {
        Object invoke(A param1, B param2);
    }
}
