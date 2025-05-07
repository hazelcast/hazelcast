/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.tpcengine.util.ReflectionUtil;

import java.lang.invoke.VarHandle;

/**
 * The implementation of internal indexes stats specialized for partitioned
 * indexes.
 * <p>
 * The main trait of the implementation is the lack of concurrency support since
 * partitioned indexes and their stats are updated from a single thread only.
 */
public class PartitionIndexesStats implements IndexesStats {

    private static final VarHandle QUERY_COUNT = ReflectionUtil.findVarHandle("queryCount", long.class);
    private static final VarHandle INDEXED_QUERY_COUNT = ReflectionUtil.findVarHandle("indexedQueryCount", long.class);
    private static final VarHandle INDEXES_SKIPPED_QUERY_COUNT =
            ReflectionUtil.findVarHandle("indexesSkippedQueryCount", long.class);
    private static final VarHandle NO_MATCHING_INDEX_QUERY_COUNT =
            ReflectionUtil.findVarHandle("noMatchingIndexQueryCount", long.class);

    private volatile long queryCount;
    private volatile long indexedQueryCount;
    private volatile long indexesSkippedQueryCount;
    private volatile long noMatchingIndexQueryCount;

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        QUERY_COUNT.setOpaque(this, queryCount + 1);
    }

    @Override
    public long getIndexedQueryCount() {
        return indexedQueryCount;
    }

    @Override
    public void incrementIndexedQueryCount() {
        INDEXED_QUERY_COUNT.setOpaque(this, indexedQueryCount + 1);
    }

    @Override
    public long getIndexesSkippedQueryCount() {
        return indexesSkippedQueryCount;
    }

    @Override
    public void incrementIndexesSkippedQueryCount() {
        INDEXES_SKIPPED_QUERY_COUNT.setOpaque(this, indexesSkippedQueryCount + 1);
    }

    @Override
    public long getNoMatchingIndexQueryCount() {
        return noMatchingIndexQueryCount;
    }

    @Override
    public void incrementNoMatchingIndexQueryCount() {
        NO_MATCHING_INDEX_QUERY_COUNT.setOpaque(this, noMatchingIndexQueryCount + 1);
    }

    @Override
    public PerIndexStats createPerIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
        return new PartitionPerIndexStats();
    }

}
