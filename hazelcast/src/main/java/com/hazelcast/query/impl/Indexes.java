/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.query.IndexProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.Extractors;

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
    private final InternalSerializationService serializationService;
    private final IndexProvider indexProvider;
    private final Extractors extractors;
    private final boolean global;


    public Indexes(InternalSerializationService serializationService, IndexProvider indexProvider,
                   Extractors extractors, boolean global) {
        this.serializationService = serializationService;
        this.indexProvider = indexProvider;
        this.extractors = extractors;
        this.global = global;
    }

    public synchronized Index destroyIndex(String attribute) {
        return mapIndexes.remove(attribute);
    }

    public synchronized Index addOrGetIndex(String attribute, boolean ordered) {
        Index index = mapIndexes.get(attribute);
        if (index != null) {
            return index;
        }
        index = indexProvider.createIndex(attribute, ordered, extractors, serializationService);
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
        for (Index index : getIndexes()) {
            index.destroy();
        }

        indexes.set(EMPTY_INDEX);
        mapIndexes.clear();
        hasIndex = false;
    }

    public void removeEntryIndex(Data key, Object value) throws QueryException {
        Index[] indexes = getIndexes();
        for (Index index : indexes) {
            index.removeEntryIndex(key, value);
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
     * @return true if the index is global-per map, meaning there is just a single instance of this object per map.
     * Global indexes are used in on-heap maps, since they give a significant performance boost.
     * The opposite of global indexes are partitioned-indexes which are stored locally per partition.
     * In case of a partitioned-index, each query has to query the index in each partition separately, which all-together
     * may be around 3 times slower than querying a single global index.
     */
    public boolean isGlobal() {
        return global;
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
