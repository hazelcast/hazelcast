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

package com.hazelcast.query;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.*;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class IndexService {

    private static final Index[] EMPTY_ARRAY = new Index[0];

    private final ConcurrentMap<String, Index> mapIndexes = new ConcurrentHashMap<String, Index>(3);

    /**
     * Does not need to be volatile since guarded (picky backed) on hasIndex
     */
    private Index[] indexes;

    private volatile boolean hasIndex = false;

    public synchronized Index destroyIndex(String attribute) {
        return mapIndexes.remove(attribute);
    }

    public Index addOrGetIndex(String attribute, boolean ordered) {
        Index index = mapIndexes.get(attribute);
        if (index != null) return index;
        index = buildIndex(attribute, ordered);
        Index old = mapIndexes.putIfAbsent(attribute, index);
        if (old == null) {
            Collection<Index> values = mapIndexes.values();
            Index[] indexes = values.toArray(new Index[values.size()]);

            // Do not reorder the other next two statements we need the
            // volatile guarantee
            this.indexes = indexes;
            hasIndex = true;
        }
        return old;
    }

    public Index[] getIndexes() {
        // Don't remove this it's a guard access for the indexes
        if (hasIndex) {
            return indexes;
        }
        return EMPTY_ARRAY;
    }

    public Index getIndex(String attribute) {
        return mapIndexes.get(attribute);
    }

    public void removeEntryIndex(Data indexKey) throws QueryException {
        // Don't remove this it's a guard access for the indexes
        if (hasIndex) {
            for (Index index : indexes) {
                index.removeEntryIndex(indexKey);
            }
        }
    }

    public void saveEntryIndex(QueryableEntry queryableEntry) throws QueryException {
        // Don't remove this it's a guard access for the indexes
        if (hasIndex) {
            for (Index index : indexes) {
                index.saveEntryIndex(queryableEntry);
            }
        }
    }

    public boolean hasIndex() {
        return hasIndex;
    }

    public Set<QueryableEntry> query(Predicate predicate) {
        // Don't remove this it's a guard access for the indexes
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

    protected abstract Index buildIndex(String attribute, boolean ordered);

}
