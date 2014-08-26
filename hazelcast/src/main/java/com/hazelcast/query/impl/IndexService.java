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

import com.hazelcast.monitor.IndexStats;
import com.hazelcast.monitor.impl.MapIndexStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.util.ValidationUtil;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class contains methods which manipulate and access index.
 */
public class IndexService {
    private final AtomicReference<Index[]> indexes = new AtomicReference<Index[]>();
    private volatile boolean hasIndex;
    private MapIndexStats localMapIndexStats;

    public synchronized Index destroyIndex(String attribute, Predicate predicate) {
        Index[] idxs = indexes.get();
        Index[] newIndexes = new Index[idxs.length - 1];
        Integer indexOfIndex = null;
        for (int i = 0; i < newIndexes.length; i++) {
            Index idx = idxs[i];

            if (idx.getAttributeName().equals(attribute) && idx.getPredicate().equals(predicate)) {
                indexOfIndex = i;
                if (localMapIndexStats != null) {
                    localMapIndexStats.removeIndex(indexOfIndex);
                }
                continue;
            }

            if (indexOfIndex == null) {
                newIndexes[i] = idx;
            } else {
                newIndexes[i - 1] = idx;
            }
        }
        if (indexOfIndex != null) {
            indexes.set(newIndexes);
        } else {
            return idxs[indexOfIndex];
        }
        return null;
    }

    public MapIndexStats getLocalMapIndexStats() {
        return localMapIndexStats;
    }


    public IndexStats[] createStatistics() {
        if (localMapIndexStats == null) {
            return null;
        }
        Index[] idxs = indexes.get();
        int length = idxs == null ? 0 : idxs.length;

        ArrayList<IndexStats> objects = new ArrayList<IndexStats>(length);
        for (int i = 0; i < length; i++) {
            Index idx = idxs[i];
            MapIndexStats.IndexStatsImpl s = new MapIndexStats
                    .IndexStatsImpl(idx.getRecordCount(), localMapIndexStats.getIndexUsageCount(i),
                    idx.getPredicate(), idx.getAttributeName());
            objects.add(s);
        }

        return objects.toArray(new IndexStats[objects.size()]);
    }

    private Index findIndex(String attribute, Predicate predicate) {
        Index[] idxs = indexes.get();
        if (idxs == null) {
            return null;
        }
        for (Index idx : idxs) {
            if (idx.getAttributeName().equals(attribute) && ValidationUtil.equalOrNull(idx.getPredicate(), predicate)) {
                return idx;
            }
        }
        return null;
    }

    public synchronized Index addOrGetIndex(String attribute, boolean ordered, final Predicate predicate) {
        Index index = findIndex(attribute, predicate);
        if (index != null) {
            return index;
        }
        index = new IndexImpl(attribute, ordered, predicate);

        Index[] oldIndexes = indexes.get();

        final int length = oldIndexes == null ? 0 : oldIndexes.length;
        Index[] newIndexes = new Index[length + 1];
        for (int i = 0; i < length; i++) {
            newIndexes[i] = oldIndexes[i];
        }
        newIndexes[length] = index;
        if (localMapIndexStats != null) {
            index.setStatistics(localMapIndexStats.createIndexUsageIncrementer(length));
        }

        indexes.set(newIndexes);
        hasIndex = true;
        if (localMapIndexStats != null) {
            localMapIndexStats.addIndex(length);
        }
        return index;
    }

    public synchronized Index addOrGetIndex(String attribute, boolean ordered) {
        return addOrGetIndex(attribute, ordered, null);
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

    public Set<QueryableEntry> query(Predicate predicate) {

        if (hasIndex) {
            if (predicate instanceof IndexAwarePredicate) {
                QueryContext queryContext = new QueryContext(this, (IndexAwarePredicate) predicate);
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                if (iap.isIndexed(queryContext)) {
                    return iap.filter(queryContext);
                }
            }
        }
        return null;
    }

    public void enableStatistics() {
        this.localMapIndexStats = new MapIndexStats();
    }
}
