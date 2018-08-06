/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.monitor.impl.InternalIndexStats;

import java.util.HashSet;

/**
 * Extends the basic query context to support the per-index stats tracking on
 * behalf of partitioned indexes.
 */
public class PartitionQueryContextWithStats extends QueryContext {

    private final HashSet<InternalIndexStats> trackedStats = new HashSet<InternalIndexStats>(8);

    /**
     * Constructs a new partition query context with stats for the given indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextWithStats(Indexes indexes) {
        super(indexes);
    }

    @Override
    void attachTo(Indexes indexes) {
        assert indexes == this.indexes;
        for (InternalIndexStats stats : trackedStats) {
            stats.resetPerQueryStats();
        }
        trackedStats.clear();
    }

    @Override
    public Index getIndex(String attributeName) {
        if (indexes == null) {
            return null;
        }

        InternalIndex index = indexes.getIndex(attributeName);
        if (index == null) {
            return null;
        }

        trackedStats.add(index.getIndexStats());

        return index;
    }

    @Override
    void applyPerQueryStats() {
        for (InternalIndexStats stats : trackedStats) {
            stats.incrementQueryCount();
        }
    }

}
