/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.extractor.MultiResult;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for concrete index store implementations
 */
public abstract class BaseIndexStore implements IndexStore {

    protected static final float LOAD_FACTOR = 0.75F;

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    protected ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

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
            List<Object> results = ((MultiResult) newValue).getResults();
            for (Object o : results) {
                Comparable sanitizedValue = sanitizeValue((Comparable) o);
                newIndexInternal(sanitizedValue, record);
            }
        } else {
            Comparable sanitizeValue = sanitizeValue((Comparable) newValue);
            newIndexInternal(sanitizeValue, record);
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
                removeIndexInternal((Comparable) o, indexKey);
            }
        } else {
            removeIndexInternal((Comparable) oldValue, indexKey);
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


    void takeWriteLock() {
        writeLock.lock();
    }

    void releaseWriteLock() {
        writeLock.unlock();
    }

    protected void takeReadLock() {
        readLock.lock();
    }

    protected void releaseReadLock() {
        readLock.unlock();
    }

    private Comparable sanitizeValue(Comparable value) {
        if (value == null) {
            value = IndexImpl.NULL;
        } else if (value.getClass().isEnum()) {
            value = TypeConverters.ENUM_CONVERTER.convert(value);
        }
        return value;
    }
}
