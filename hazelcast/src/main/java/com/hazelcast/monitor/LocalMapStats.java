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
     * Returns the number of hits (reads) of the locally owned entries.
     *
     * @return number of hits (reads).
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
     * @return
     */
    long getDirtyEntryCount();
}
