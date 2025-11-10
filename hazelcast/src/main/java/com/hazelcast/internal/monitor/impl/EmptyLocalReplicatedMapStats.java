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

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.partition.LocalReplicationStats;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;

import java.util.Map;

/**
 * Implementation of {@link LocalReplicatedMapStats} with empty and immutable
 * inner state. This is used when the statistic is
 * disabled in {@link ReplicatedMapConfig}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class EmptyLocalReplicatedMapStats implements LocalReplicatedMapStats {

    public EmptyLocalReplicatedMapStats() {
    }

    @Override
    public long getOwnedEntryCount() {
        return 0;
    }

    @Override
    public long getBackupEntryCount() {
        return 0;
    }

    @Override
    public int getBackupCount() {
        return 0;
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return 0;
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return 0;
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public long getLastAccessTime() {
        return 0;
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
    }

    @Override
    public long getHits() {
        return 0;
    }

    @Override
    public long getEvictionCount() {
        return 0;
    }

    @Override
    public long getExpirationCount() {
        return 0;
    }

    @Override
    public long getLockedEntryCount() {
        return 0;
    }

    @Override
    public long getDirtyEntryCount() {
        return 0;
    }

    @Override
    public long getPutOperationCount() {
        return 0;
    }

    @Override
    public long getGetOperationCount() {
        return 0;
    }

    @Override
    public long getRemoveOperationCount() {
        return 0;
    }

    @Override
    public long getValuesCallCount() {
        return 0;
    }

    @Override
    public long getEntrySetCallCount() {
        return 0;
    }

    @Override
    public long getTotalPutLatency() {
        return 0;
    }

    @Override
    public long getTotalGetLatency() {
        return 0;
    }

    @Override
    public long getTotalRemoveLatency() {
        return 0;
    }

    @Override
    public long getMaxPutLatency() {
        return 0;
    }

    @Override
    public long getMaxGetLatency() {
        return 0;
    }

    @Override
    public long getMaxRemoveLatency() {
        return 0;
    }

    @Override
    public long getEventOperationCount() {
        return 0;
    }

    @Override
    public long getOtherOperationCount() {
        return 0;
    }

    @Override
    public long total() {
        return 0;
    }

    @Override
    public long getHeapCost() {
        return 0;
    }

    @Override
    public long getMerkleTreesCost() {
        return 0;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        throw new UnsupportedOperationException("Replicated map has no Near Cache!");
    }

    @Override
    public long getQueryCount() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public long getIndexedQueryCount() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public Map<String, LocalIndexStats> getIndexStats() {
        throw new UnsupportedOperationException("Queries on replicated maps are not supported.");
    }

    @Override
    public long getSetOperationCount() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
    }

    @Override
    public long getTotalSetLatency() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
    }

    @Override
    public long getMaxSetLatency() {
        throw new UnsupportedOperationException("Set operation on replicated maps is not supported.");
    }

    @Override
    public LocalReplicationStats getReplicationStats() {
        throw new UnsupportedOperationException("Replication stats are not available for replicated maps.");
    }

    @Override
    public String toString() {
        return "LocalReplicatedMapStatsImpl{"
                + "lastAccessTime=0"
                + ", lastUpdateTime=0"
                + ", hits=0"
                + ", numberOfOtherOperations=0"
                + ", numberOfEvents=0"
                + ", getCount=0"
                + ", putCount=0"
                + ", removeCount=0"
                + ", totalGetLatencies=0"
                + ", totalPutLatencies=0"
                + ", totalRemoveLatencies=0"
                + ", ownedEntryCount=0"
                + ", ownedEntryMemoryCost=0"
                + ", creationTime=0"
                + '}';
    }
}
