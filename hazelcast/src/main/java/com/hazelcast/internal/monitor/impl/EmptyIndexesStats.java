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

public class EmptyIndexesStats
        implements IndexesStats {
    @Override
    public long getQueryCount() {
        return 0;
    }

    @Override
    public void incrementQueryCount() {
        // do nothing
    }

    @Override
    public long getIndexedQueryCount() {
        return 0;
    }

    @Override
    public void incrementIndexedQueryCount() {
        // do nothing
    }

    @Override
    public long getIndexesSkippedQueryCount() {
        return 0;
    }

    @Override
    public void incrementIndexesSkippedQueryCount() {
    }

    @Override
    public long getNoMatchingIndexQueryCount() {
        return 0;
    }

    @Override
    public void incrementNoMatchingIndexQueryCount() {
    }

    @Override
    public PerIndexStats createPerIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
        return PerIndexStats.EMPTY;
    }
}
