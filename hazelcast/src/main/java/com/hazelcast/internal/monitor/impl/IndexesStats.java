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

/**
 * Provides internal statistics for {@link com.hazelcast.query.impl.Indexes
 * Indexes}.
 */
public interface IndexesStats {

    /**
     * Empty no-op internal indexes stats.
     */
    IndexesStats EMPTY = new IndexesStats() {
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
        public PerIndexStats createPerIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
            return PerIndexStats.EMPTY;
        }
    };

    /**
     * Returns the number of queries performed on the indexes.
     */
    long getQueryCount();

    /**
     * Increments the number of queries performed on the indexes.
     */
    void incrementQueryCount();

    /**
     * Returns the number of indexed queries performed on the indexes.
     */
    long getIndexedQueryCount();

    /**
     * Increments the number of indexed queries performed on the indexes.
     */
    void incrementIndexedQueryCount();

    /**
     * Creates a new instance of internal per-index stats.
     *
     * @param ordered                   {@code true} if the stats are being created
     *                                  for an ordered index, {@code false} otherwise.
     * @param queryableEntriesAreCached {@code true} if the stats are being created
     *                                  for an index for which queryable entries are
     *                                  cached, {@code false} otherwise.
     * @return the created internal per-index stats instance.
     */
    PerIndexStats createPerIndexStats(boolean ordered, boolean queryableEntriesAreCached);

}
