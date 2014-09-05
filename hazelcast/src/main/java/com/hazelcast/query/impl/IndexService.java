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
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains methods which manipulate and access index.
 */
public class IndexService {
    private final ConcurrentMap<String, Index> mapIndexes = new ConcurrentHashMap<String, Index>(3);
    private final AtomicReference<Index[]> indexes = new AtomicReference<Index[]>();
    private volatile boolean hasIndex;

    public synchronized Index destroyIndex(String attribute) {
        return mapIndexes.remove(attribute);
    }

    public synchronized Index addOrGetIndex(String attribute, boolean ordered) {
        Index index = mapIndexes.get(attribute);
        if (index != null) {
            return index;
        }
        index = new IndexImpl(attribute, ordered);
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

    public void removeEntryIndex(Data indexKey) throws QueryException {
        for (Index index : indexes.get()) {
            index.removeEntryIndex(indexKey);
        }
    }

    public boolean hasIndex() {
        return hasIndex;
    }

    public void saveEntryIndex(QueryableEntry queryableEntry) throws QueryException {
        for (Index index : indexes.get()) {
            index.saveEntryIndex(queryableEntry);
        }
    }

    Index getIndex(String attribute) {
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
