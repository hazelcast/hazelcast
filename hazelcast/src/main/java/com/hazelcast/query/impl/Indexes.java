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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contains all indexes for a data-structure, e.g. an IMap.
 */
public class Indexes {
    private static final Index[] EMPTY_INDEX = {};
    private final ConcurrentMap<String, Index> mapIndexes = new ConcurrentHashMap<String, Index>(3);
    private final AtomicReference<Index[]> indexes = new AtomicReference<Index[]>(EMPTY_INDEX);
    private volatile boolean hasIndex;
    private SerializationService ss;

    public Indexes(SerializationService ss) {
        this.ss = ss;
    }

    public synchronized Index destroyIndex(String attribute) {
        return mapIndexes.remove(attribute);
    }

    public synchronized Index addOrGetIndex(String attribute, boolean ordered) {
        Index index = mapIndexes.get(attribute);
        if (index != null) {
            return index;
        }
        index = new IndexImpl(attribute, ordered, ss);
        mapIndexes.put(attribute, index);
        Object[] indexObjects = mapIndexes.values().toArray();
        Index[] newIndexes = new Index[indexObjects.length];
        for (int i = 0; i < indexObjects.length; i++) {
            newIndexes[i] = (Index) indexObjects[i];
        }
        indexes.set(newIndexes);
        hasIndex = true;
        return index;
    }

    public Index[] getIndexes() {
        return indexes.get();
    }

    public void clearIndexes() {
        indexes.set(EMPTY_INDEX);
        mapIndexes.clear();
        hasIndex = false;
    }

    public void removeEntryIndex(Record record) throws QueryException {
        Index[] indexes = getIndexes();
        for (Index index : indexes) {
            index.removeEntryIndex(record);
        }
    }

    public boolean hasIndex() {
        return hasIndex;
    }

    public void saveEntryIndex(QueryableEntry queryableEntry, Object oldValue) throws QueryException {
        Index[] indexes = getIndexes();
        for (Index index : indexes) {
            index.saveEntryIndex(queryableEntry, oldValue);
        }
    }

    /**
     * Get index for a given attribute. If the index does not exist then returns null.
     *
     * @param attribute
     * @return Index for attribute or null if the index does not exist.
     */
    public Index getIndex(String attribute) {
        return mapIndexes.get(attribute);
    }

    public Set<QueryableEntry> query(Predicate predicate) {
        if (hasIndex) {
            QueryContext queryContext = new QueryContext(this);
            if (predicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                if (iap.isIndexed(queryContext)) {
                    return iap.filter(queryContext);
                }
            }
        }
        return null;
    }
}
