/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.stats;

@SuppressWarnings("checkstyle:methodcount")
public interface LocalVectorCollectionStats {

    /**
     * Returns the number of entries owned by this member.
     *
     * @return number of entries owned by this member.
     */
    long getOwnedEntryCount();

    /**
     * Returns the number of backup entries held by this member.
     *
     * @return number of backup entries held by this member.
     */
    long getBackupEntryCount();

    /**
     * Returns the number of backups per entry.
     *
     * @return the number of backups per entry.
     */
    int getBackupCount();

    /**
     * Returns heap memory cost (number of bytes) of owned entries in this member.
     * Entry memory cost includes key-value data, vectors, JVector index and supporting mappings
     * (e.g. nodeId mappings).
     *
     * @return heap memory cost (number of bytes) of owned entries in this member.
     */
    long getOwnedEntryHeapMemoryCost();

    /**
     * Returns heap memory cost (number of bytes) of backup entries in this member.
     * Entry memory cost includes key-value data, vectors, JVector index and supporting mappings
     * (e.g. nodeId mappings).
     *
     * @return heap memory cost (number of bytes) of backup entries in this member.
     */
    long getBackupEntryHeapMemoryCost();

    /**
     * Returns the creation time of this map on this member.
     *
     * @return creation time of this map on this member.
     */
    long getCreationTime();

    /**
     * Returns the last access (read) time of the locally owned entries.
     *
     * @return last access (read) time of the locally owned entries.
     */
    long getLastAccessTime();

    /**
     * Returns the last update time of the locally owned entries.
     *
     * @return last update time of the locally owned entries.
     */
    long getLastUpdateTime();

    /**
     * Returns the number of put operations
     *
     * @return number of put operations
     */
    long getPutOperationCount();

    /**
     * Returns the number of set operations
     *
     * @return number of set operations
     */
    long getSetOperationCount();

    /**
     * Returns the number of get operations
     *
     * @return number of get operations
     */
    long getGetOperationCount();

    /**
     * Returns the number of Remove operations
     *
     * @return number of remove operations
     */
    long getRemoveOperationCount();

    long getDeleteOperationCount();

    /**
     * Returns the total latency of put operations. To get the average latency, divide by the number of puts
     *
     * @return the total latency of put operations
     */
    long getTotalPutLatency();

    long getTotalPutAllLatency();

    /**
     * Returns the total latency of set operations. To get the average latency, divide by the number of sets
     *
     * @return the total latency of set operations
     */
    long getTotalSetLatency();

    /**
     * Returns the total latency of get operations. To get the average latency, divide by the number of gets
     *
     * @return the total latency of get operations
     */
    long getTotalGetLatency();

    /**
     * Returns the total latency of remove operations. To get the average latency, divide by the number of gets
     *
     * @return the total latency of remove operations
     */
    long getTotalRemoveLatency();

    long getTotalDeleteLatency();

    /**
     * Returns the maximum latency of put operations.
     *
     * @return the maximum latency of put operations
     */
    long getMaxPutLatency();

    long getMaxPutAllLatency();

    /**
     * Returns the maximum latency of set operations.
     *
     * @return the maximum latency of set operations
     */
    long getMaxSetLatency();

    /**
     * Returns the maximum latency of get operations.
     *
     * @return the maximum latency of get operations
     */
    long getMaxGetLatency();

    /**
     * Returns the maximum latency of remove operations.
     *
     * @return the maximum latency of remove operations
     */
    long getMaxRemoveLatency();

    long getMaxDeleteLatency();

    /**
     * Total heap cost
     *
     * @return heap cost
     */
    long getHeapCost();

    long getSearchCount();

    long getSearchResultsCount();

    long getMaxSearchLatency();

    long getTotalSearchLatency();

    long getSearchIndexQueryCount();

    long getSearchIndexVisitedNodes();

    long getOptimizeCount();

    long getMaxOptimizeLatency();

    long getTotalOptimizeLatency();

    long getClearCount();

    long getMaxClearLatency();

    long getTotalClearLatency();

    long getSizeCount();

    long getMaxSizeLatency();

    long getTotalSizeLatency();
}
