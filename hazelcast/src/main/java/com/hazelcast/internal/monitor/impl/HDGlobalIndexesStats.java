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

package com.hazelcast.internal.monitor.impl;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal indexes stats specialized for HD global indexes.
 * <p>
 * The main trait of the implementation is the concurrency support, which is
 * required for global indexes because they are shared among partitions.
 */
public final class HDGlobalIndexesStats implements IndexesStats {

    private static final AtomicLongFieldUpdater<HDGlobalIndexesStats> QUERY_COUNT = newUpdater(HDGlobalIndexesStats.class,
            "queryCount");
    private static final AtomicLongFieldUpdater<HDGlobalIndexesStats> INDEXED_QUERY_COUNT = newUpdater(HDGlobalIndexesStats.class,
            "indexedQueryCount");

    private volatile long queryCount;
    private volatile long indexedQueryCount;

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    @Override
    public void incrementQueryCount() {
        QUERY_COUNT.incrementAndGet(this);
    }

    @Override
    public long getIndexedQueryCount() {
        return indexedQueryCount;
    }

    @Override
    public void incrementIndexedQueryCount() {
        INDEXED_QUERY_COUNT.incrementAndGet(this);
    }

    @Override
    public PerIndexStats createPerIndexStats(boolean ordered, boolean usesCachedQueryableEntries) {
        return new HDGlobalPerIndexStats(ordered, usesCachedQueryableEntries);
    }
}
