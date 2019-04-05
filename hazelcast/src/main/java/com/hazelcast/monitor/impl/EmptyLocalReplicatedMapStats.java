/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.NearCacheStats;

import java.util.Map;

/**
 * Implementation of {@link LocalReplicatedMapStats} with empty and immutable inner state. This is used when the statistic is
 * disabled in {@link ReplicatedMapConfig}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class EmptyLocalReplicatedMapStats implements LocalReplicatedMapStats {

    private final JsonObject jsonObject;

    public EmptyLocalReplicatedMapStats() {
        JsonObject root = new JsonObject();
        root.add("getCount", 0L);
        root.add("putCount", 0L);
        root.add("removeCount", 0L);
        root.add("numberOfOtherOperations", 0L);
        root.add("numberOfEvents", 0L);
        root.add("lastAccessTime", 0L);
        root.add("lastUpdateTime", 0L);
        root.add("hits", 0L);
        root.add("ownedEntryCount", 0L);
        root.add("ownedEntryMemoryCost", 0L);
        root.add("creationTime", 0L);
        root.add("totalGetLatencies", 0L);
        root.add("totalPutLatencies", 0L);
        root.add("totalRemoveLatencies", 0L);
        root.add("maxGetLatency", 0L);
        root.add("maxPutLatency", 0L);
        root.add("maxRemoveLatency", 0L);
        jsonObject = root;
    }

    @Override
    public long getReplicationEventCount() {
        return 0;
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
    public JsonObject toJson() {
        return jsonObject;
    }

    @Override
    public void fromJson(JsonObject json) {

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
