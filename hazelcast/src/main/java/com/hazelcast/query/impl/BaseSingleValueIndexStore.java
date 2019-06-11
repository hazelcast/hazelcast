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

import java.util.List;

public abstract class BaseSingleValueIndexStore extends BaseIndexStore {

    private boolean multiResultHasToDetectDuplicates;

    BaseSingleValueIndexStore(IndexCopyBehavior copyOn) {
        super(copyOn);
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
    public final void remove(Object value, Data entryKey, Object entryValue, IndexOperationStats operationStats) {
        takeWriteLock();
        try {
            unwrapAndRemoveFromIndex(value, entryKey, operationStats);
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

}
