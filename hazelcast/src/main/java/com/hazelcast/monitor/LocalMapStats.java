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

package com.hazelcast.monitor;

/**
 * Local map statistics. As everything is partitioned in Hazelcast,
 * each member owns 1/N (N being the number of members in the cluster)
 * entries of a distributed map. Each member also holds backup entries
 * of another member. LocalMapStats tells you the count of owned and backup
 * entries besides their size in memory.
 * <p/>
 * When an entry is removed, it is not erased from the map immediately.
 * Hazelcast will mark it as removed and erase it couple of seconds later for
 * correct versioning of backups.
 */
public interface LocalMapStats extends LocalInstanceStats<LocalMapOperationStats> {

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
     * Returns the number of marked as removed entries in this member.
     * <p/>
     * When an entry is removed, it is not erased from the map immediately.
     * Hazelcast will mark it as removed and erase it couple of seconds later for
     * correct versioning of backups.
     *
     * @return number of entries marked as removed.
     */
    long getMarkedAsRemovedEntryCount();

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
     * Returns memory cost (number of bytes) of marked as
     * removed entries in this member.
     *
     * @return memory cost (number of bytes) of marked as removed entries.
     */
    long getMarkedAsRemovedMemoryCost();

    /**
     * Returns the creation time of this map on this member.
     *
     * @return creation time of this map on this member.
     */
    long getCreationTime();

    /**
     * Returns the last access (read) time of the locally owned entries.
     *
     * @return last access time.
     */
    long getLastAccessTime();

    /**
     * Returns the last update time of the locally owned entries.
     *
     * @return last update time.
     */
    long getLastUpdateTime();

    /**
     * Returns the last eviction time of the locally owned entries.
     *
     * @return last eviction time.
     */
    long getLastEvictionTime();

    /**
     * Returns the number of hits (reads) of the locally owned entries.
     *
     * @return number of hits (reads).
     */
    long getHits();

    /**
     * Returns the number of misses (get returns null) of the locally owned entries.
     *
     * @return number of misses.
     */
    long getMisses();

    /**
     * Returns the number of currently locked locally owned keys.
     *
     * @return number of locked entries.
     */
    long getLockedEntryCount();

    /**
     * Returns the number of cluster-wide threads waiting
     * to acquire locks for the locally owned keys.
     *
     * @return number of threads waiting for locks.
     */
    long getLockWaitCount();

    /**
     * Returns the number of entries that the member owns and are dirty (updated but not persisted yet).
     * dirty entry count is meaningful when there is a persistence defined.
     *
     * @return
     */
    long getDirtyEntryCount();
}
