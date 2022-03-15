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

import com.hazelcast.internal.monitor.impl.PerIndexStats;

import java.util.HashSet;

/**
 * Extends the basic query context to support the per-index stats tracking on
 * behalf of partitioned indexes.
 */
public class PartitionQueryContextWithStats extends QueryContext {

    private final HashSet<PerIndexStats> trackedStats = new HashSet<PerIndexStats>(8);

    /**
     * Constructs a new partition query context with stats for the given indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextWithStats(Indexes indexes) {
        super(indexes, 1);
    }

    @Override
    void attachTo(Indexes indexes, int ownedPartitionCount) {
        assert indexes == this.indexes;
        assert ownedPartitionCount == 1 && this.ownedPartitionCount == 1;
        for (PerIndexStats stats : trackedStats) {
            stats.resetPerQueryStats();
        }
        trackedStats.clear();
    }

    @Override
    void applyPerQueryStats() {
        for (PerIndexStats stats : trackedStats) {
            stats.incrementQueryCount();
        }
    }

    @Override
    public Index matchIndex(String pattern, IndexMatchHint matchHint) {
        InternalIndex index = indexes.matchIndex(pattern, matchHint, ownedPartitionCount);
        if (index == null) {
            return null;
        }

        trackedStats.add(index.getPerIndexStats());

        return index;
    }

}
