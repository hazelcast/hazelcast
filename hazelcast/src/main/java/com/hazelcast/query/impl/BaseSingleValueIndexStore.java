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

import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.getters.MultiResult;

import java.util.List;

/**
 * The base store for indexes that are unable to work with multi-value
 * attributes natively. For such indexes {@link MultiResult}s are split into
 * individual values and each value is inserted/removed separately.
 * <p>
 * All operations on the off-heap index store like insert/remove/getRecords are
 * not guarded by a global lock. The subclasses must either implement thread-safe
 * access operations or the index should be used as single-threaded by design.
 */
public abstract class BaseSingleValueIndexStore extends BaseIndexStore {

    /**
     * The flag is set to {@code true} when a collection is inserted into the index
     * and deduplication is needed.
     * <p>
     * Since there is no global lock guarding update/getRecords() operations
     * some queries still can return duplicates.
     */
    private volatile boolean multiResultHasToDetectDuplicates;

    BaseSingleValueIndexStore(IndexCopyBehavior copyOn, boolean enableGlobalLock) {
        super(copyOn, enableGlobalLock);
    }

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

    final MultiResultSet createMultiResultSet() {
        return multiResultHasToDetectDuplicates ? new DuplicateDetectingMultiResult() : new FastMultiResultSet();
    }

    @Override
    public final void insert(Object value, CachedQueryEntry entry, QueryableEntry entryToStore,
                             IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndInsertToIndex(value, entryToStore, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public final void update(Object oldValue, Object newValue, CachedQueryEntry entry, QueryableEntry entryToStore,
                             IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            Data indexKey = entry.getKeyData();
            unwrapAndRemoveFromIndex(oldValue, indexKey, operationStats);
            unwrapAndInsertToIndex(newValue, entryToStore, operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public final void remove(Object value, CachedQueryEntry entry, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndRemoveFromIndex(value, entry.getKeyData(), operationStats);
        } finally {
            releaseWriteLock();
        }
    }

    private void unwrapAndInsertToIndex(Object newValue, QueryableEntry<?, ?> queryableEntry,
                                        IndexOperationStats operationStats) {
        if (newValue == NonTerminalJsonValue.INSTANCE) {
            return;
        }
        if (newValue instanceof MultiResult) {
            multiResultHasToDetectDuplicates = true;
            List<?> results = ((MultiResult<?>) newValue).getResults();
            for (Object o : results) {
                Comparable<?> sanitizedValue = sanitizeValue(o);
                Object oldValue = insertInternal(sanitizedValue, queryableEntry);
                if (oldValue == null) {
                    operationStats.onEntryAdded(newValue);
                }
            }
        } else {
            Comparable<?> sanitizedValue = sanitizeValue(newValue);
            Object oldValue = insertInternal(sanitizedValue, queryableEntry);
            if (oldValue == null) {
                operationStats.onEntryAdded(newValue);
            }
        }
    }

    private void unwrapAndRemoveFromIndex(Object oldValue, Data recordKey, IndexOperationStats operationStats) {
        if (oldValue == NonTerminalJsonValue.INSTANCE) {
            return;
        }
        if (oldValue instanceof MultiResult) {
            List<?> results = ((MultiResult<?>) oldValue).getResults();
            for (Object o : results) {
                Comparable<?> sanitizedValue = sanitizeValue(o);
                Object removedValue = removeInternal(sanitizedValue, recordKey);
                if (removedValue != null) {
                    operationStats.onEntryRemoved(oldValue);
                }
            }
        } else {
            Comparable<?> sanitizedValue = sanitizeValue(oldValue);
            Object removedValue = removeInternal(sanitizedValue, recordKey);
            if (removedValue != null) {
                operationStats.onEntryRemoved(oldValue);
            }
        }
    }

}
