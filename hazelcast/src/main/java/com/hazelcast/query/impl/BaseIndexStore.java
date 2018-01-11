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

    abstract void newIndexInternal(Comparable newValue, QueryableEntry record);

    abstract void removeIndexInternal(Comparable oldValue, Data indexKey);

    @Override
    public final void newIndex(Object newValue, QueryableEntry record) {
        takeWriteLock();
        try {
            unwrapAndAddToIndex(newValue, record);
        } finally {
            releaseWriteLock();
        }
    }

    private void unwrapAndAddToIndex(Object newValue, QueryableEntry record) {
        if (newValue instanceof MultiResult) {
            multiResultHasToDetectDuplicates = true;
            List<Object> results = ((MultiResult) newValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                newIndexInternal(sanitizedValue, record);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(newValue);
            newIndexInternal(sanitizedValue, record);
        }
    }

    @Override
    public final void removeIndex(Object oldValue, Data indexKey) {
        takeWriteLock();
        try {
            unwrapAndRemoveFromIndex(oldValue, indexKey);
        } finally {
            releaseWriteLock();
        }
    }

    private void unwrapAndRemoveFromIndex(Object oldValue, Data indexKey) {
        if (oldValue instanceof MultiResult) {
            List<Object> results = ((MultiResult) oldValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue(o);
                removeIndexInternal(sanitizedValue, indexKey);
            }
        } else {
            Comparable sanitizedValue = sanitizeValue(oldValue);
            removeIndexInternal(sanitizedValue, indexKey);
        }
    }

    @Override
    public final void updateIndex(Object oldValue, Object newValue, QueryableEntry entry) {
        takeWriteLock();
        try {
            Data indexKey = entry.getKeyData();
            unwrapAndRemoveFromIndex(oldValue, indexKey);
            unwrapAndAddToIndex(newValue, entry);
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
            throw new IllegalArgumentException("It is not allowed to used a type that is not Comparable: "
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
        void invoke(A param1, B param2);
    }
}
