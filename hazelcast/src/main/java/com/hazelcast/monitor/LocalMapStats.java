/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

/**
 * Local map statistics. As everything is partitioned in Hazelcast,
 * each member owns 1/N (N being the number of members in the cluster)
 * entries of a distributed map. Each member also holds backup entries
 * of other members. LocalMapStats tells you the count of owned and backup
 * entries besides their size in memory.
 */
public interface LocalMapStats extends LocalInstanceStats {

    /**
     * Returns the number of entries owned by this member.
     *
     * @return number of entries owned by this member.
     */
    long getOwnedEntryCount();

    /**
     * Returns the number of backup entries hold by this member.
     *
     * @return number of backup entries hold by this member.
     */
    long getBackupEntryCount();

    /**
     * Returns the number of backups per entry.
     *
     * @return the number of backups per entry.
     */
    int getBackupCount();

    /**
     * Returns memory cost (number of bytes) of owned entries in this member.
     *
     * @return memory cost (number of bytes) of owned entries in this member.
     */
    long getOwnedEntryMemoryCost();

    /**
     * Returns memory cost (number of bytes) of backup entries in this member.
     *
     * @return memory cost (number of bytes) of backup entries in this member.
     */
    long getBackupEntryMemoryCost();

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
     * Returns the number of hits (reads) of the locally owned entries.
     *
     * @return number of hits (reads) of the locally owned entries.
     */
    long getHits();

    /**
     * Returns the number of currently locked locally owned keys.
     *
     * @return number of locked entries.
     */
    long getLockedEntryCount();

    /**
     * Returns the number of entries that the member owns and are dirty (updated but not persisted yet).
     * dirty entry count is meaningful when there is a persistence defined.
     *
     * @return the number of dirty entries that the member owns
     */
    long getDirtyEntryCount();

    /**
     * Returns the number of put operations
     *
     * @return number of put operations
     */
    long getPutOperationCount();

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


    /**
     * Returns the total latency of put operations. To get the average latency, divide by the number of puts
     *
     * @return the total latency of put operations
     */
    long getTotalPutLatency();

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

    /**
     * Returns the maximum latency of put operations.
     *
     * @return the maximum latency of put operations
     */
    long getMaxPutLatency();

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

    /**
     * Returns the number of Events Received
     *
     * @return number of events received
     */
    long getEventOperationCount();

    /**
     * Returns the total number of Other Operations
     *
     * @return number of other operations
     */
    long getOtherOperationCount();

    /**
     * Returns the total number of total operations
     *
     * @return number of total operations
     */
    long total();

    /**
     * Cost of map & near cache  & backup in bytes
     * todo in object mode object size is zero.
     *
     * @return heap cost
     */
    long getHeapCost();

    /**
     * Returns statistics related to the Near Cache.
     *
     * @return statistics object for the Near Cache
     */
    NearCacheStats getNearCacheStats();


}
