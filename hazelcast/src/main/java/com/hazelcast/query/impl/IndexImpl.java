/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.QueryException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation for {@link com.hazelcast.query.impl.Index}
 */
public class IndexImpl implements Index {

    /**
     * Creates instance from NullObject is a inner class.
     */
    public static final NullObject NULL = new NullObject();

    // indexKey -- indexValue
    private final ConcurrentMap<Data, Comparable> recordValues = new ConcurrentHashMap<Data, Comparable>(1000);
    private final IndexStore indexStore;
    private final String attribute;
    private final boolean ordered;

    private volatile AttributeType attributeType;

    // "indexSyncLock" monitor is used for synchronizing method entrances of read and write operations
    private final Object indexSyncLock = new Object();
    // "indexReadLock" monitor is used for synchronizing read operations on index store
    private final Object indexReadLock = new Object();
    // "indexReadLock" monitor is used for synchronizing write operations on index store
    private final Object indexWriteLock = new Object();
    // Number of reader (query maker) count on index store
    private final AtomicLong readerCount = new AtomicLong();
    // Flag to hold status of index store is locked or not
    private volatile boolean indexLocked;

    public IndexImpl(String attribute, boolean ordered) {
        this.attribute = attribute;
        this.ordered = ordered;
        indexStore = (ordered) ? new SortedIndexStore() : new UnsortedIndexStore();
    }

    @Override
    public void removeEntryIndex(Data indexKey) {
        Comparable oldValue = recordValues.remove(indexKey);
        if (oldValue != null) {
            indexStore.removeIndex(oldValue, indexKey);
        }
    }

    @Override
    public void clear() {
        recordValues.clear();
        indexStore.clear();
        // Clear attribute type
        attributeType = null;
    }

    ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue) {
        return indexStore.getRecordMap(indexValue);
    }

    private void beforeWrite() {
        synchronized (indexSyncLock) {
            // Make index store locked so no new readers (query makers) can work on index store.
            indexLocked = true;
            // If there is active readers (query makers), wait them to finish.
            while (readerCount.get() > 0) {
                try {
                    indexReadLock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void afterWrite() {
        synchronized (indexWriteLock) {
            // Unlock index store so new readers (query makers) can work on index store.
            indexLocked = false;
            // Write has finished so notify all waiters (readers) waiting for index store lock.
            indexWriteLock.notifyAll();
        }
    }

    private void beforeRead() {
        synchronized (indexSyncLock) {
            // Wait until index is locked.
            // Index is locked by a writer so wait writer to finish its write operation on index store.
            while (indexLocked) {
                try {
                    indexWriteLock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // Read has started so increase count of readers
            readerCount.incrementAndGet();
        }
    }

    private void afterRead() {
        synchronized (indexReadLock) {
            // Read has finished so decrease count of readers
            readerCount.decrementAndGet();
            // Read has finished so notify all waiters (writers)
            // waiting for finishing active read operations
            indexReadLock.notifyAll();
        }
    }

    @Override
    public synchronized void saveEntryIndex(QueryableEntry e) throws QueryException {
        beforeWrite();
        try {
            /*
             * At first, check if attribute type is not initialized, initialize it before saving an entry index
             * Because, if entity index is saved before,
             * that thread can be blocked before executing attribute type setting code block,
             * another thread can query over indexes without knowing attribute type and
             * this causes to class cast exceptions.
             */
            if (attributeType == null) {
                // Initialize attribute type by using entry index
                attributeType = e.getAttributeType(attribute);
            }
            Data key = e.getIndexKey();
            Comparable oldValue = recordValues.remove(key);
            Comparable newValue = e.getAttribute(attribute);
            if (newValue == null) {
                newValue = NULL;
            } else if (newValue.getClass().isEnum()) {
                newValue = TypeConverters.ENUM_CONVERTER.convert(newValue);
            }
            recordValues.put(key, newValue);
            if (oldValue == null) {
                // new
                indexStore.newIndex(newValue, e);
            } else {
                // update
                indexStore.removeIndex(oldValue, key);
                indexStore.newIndex(newValue, e);
            }
        } finally {
            afterWrite();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        beforeRead();
        try {
            if (values.length == 1) {
                if (attributeType != null) {
                    return indexStore.getRecords(convert(values[0]));
                } else {
                    return new SingleResultSet(null);
                }
            } else {
                MultiResultSet results = new MultiResultSet();
                if (attributeType != null) {
                    Set<Comparable> convertedValues = new HashSet<Comparable>(values.length);
                    for (Comparable value : values) {
                        convertedValues.add(convert(value));
                    }
                    indexStore.getRecords(results, convertedValues);
                }
                return results;
            }
        } finally {
            afterRead();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        beforeRead();
        try {
            if (attributeType != null) {
                return indexStore.getRecords(convert(value));
            } else {
                return new SingleResultSet(null);
            }
        } finally {
            afterRead();
        }
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        beforeRead();
        try {
            MultiResultSet results = new MultiResultSet();
            if (attributeType != null) {
                indexStore.getSubRecordsBetween(results, convert(from), convert(to));
            }
            return results;
        } finally {
            afterRead();
        }
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        beforeRead();
        try {
            MultiResultSet results = new MultiResultSet();
            if (attributeType != null) {
                indexStore.getSubRecords(results, comparisonType, convert(searchedValue));
            }
            return results;
        } finally {
            afterRead();
        }
    }

    private Comparable convert(Comparable value) {
        if (attributeType == null) {
            return value;
        }
        return attributeType.getConverter().convert(value);
    }

    public ConcurrentMap<Data, Comparable> getRecordValues() {
        return recordValues;
    }

    @Override
    public String getAttributeName() {
        return attribute;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Provides comparable null object.
     */
    public static final class NullObject implements Comparable {
        @Override
        public int compareTo(Object o) {
            if (o == this || o instanceof NullObject) {
                return 0;
            }
            return -1;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return true;
        }
    }
}
