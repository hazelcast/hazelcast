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

package com.hazelcast.map;

import com.hazelcast.partition.LocalReplicationStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.monitor.MemberState;
import com.hazelcast.nearcache.NearCacheStats;

import java.util.Map;

/**
 * Local map statistics to be used by {@link MemberState} implementations.
 * <p>
 * As {@link IMap} is a partitioned data structure in
 * Hazelcast, each member owns a fraction of the total number of entries of a
 * distributed map.
 * <p>
 * Depending on the {@link IMap}'s configuration, each
 * member may also hold backup entries of other members. LocalMapStats
 * provides the count of owned and backup entries besides their size in memory.
 */
@SuppressWarnings({"checkstyle:methodcount"})
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
     * Returns the number of hits (reads) of locally owned entries, including those
     * which are no longer in the map (for example, may have been evicted).
     * <p>
     * The number of hits may be inaccurate after a partition is migrated to a new
     * owner member.
     *
     * @return number of hits (reads) of the locally owned entries.
     */
    long getHits();

    /**
     * Returns the number of currently locked keys. The returned count
     * includes locks on keys whether or not they are present in the map,
     * since it is allowed to lock on keys that are not present.
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

    /**
     * Returns the total latency of put operations. To get the average latency, divide by the number of puts
     *
     * @return the total latency of put operations
     */
    long getTotalPutLatency();

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

    /**
     * Returns the maximum latency of put operations.
     *
     * @return the maximum latency of put operations
     */
    long getMaxPutLatency();

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
     * Cost of map &amp; Near Cache &amp; backup &amp; Merkle trees in bytes
     * <p>
     * When {@link com.hazelcast.config.InMemoryFormat#OBJECT} is used, the heap cost is zero.
     *
     * @return heap cost
     */
    long getHeapCost();

    /**
     * Returns the heap cost of the Merkle trees
     *
     * @return the heap cost of the Merkle trees
     */
    long getMerkleTreesCost();

    /**
     * Returns statistics related to the Near Cache.
     *
     * @return statistics object for the Near Cache
     */
    NearCacheStats getNearCacheStats();

    /**
     * Returns the total number of queries performed on the map.
     * <p>
     * The returned value includes queries processed with and without indexes.
     *
     * @see #getIndexedQueryCount()
     */
    long getQueryCount();

    /**
     * Returns the total number of indexed queries performed on the map.
     * <p>
     * The returned value includes only queries processed using indexes. If
     * there are no indexes associated with the map, the returned value is
     * {@code 0}.
     *
     * @see #getQueryCount()
     */
    long getIndexedQueryCount();

    /**
     * Returns the per-index statistics map keyed by the index name.
     */
    Map<String, LocalIndexStats> getIndexStats();

    /**
     * @return replication statistics.
     * @since 5.0
     */
    LocalReplicationStats getReplicationStats();
}
