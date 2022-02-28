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

package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_SYNC_FINISHED_MERKLE;

public class WanMerkleSyncFinishedEvent extends AbstractWanSyncFinishedEvent {
    private final int nodesSynced;
    private final int minLeafEntryCount;
    private final int maxLeafEntryCount;
    private final double avgEntriesPerLeaf;
    private final double stdDevEntriesPerLeaf;

    @SuppressWarnings("checkstyle:parameternumber")
    public WanMerkleSyncFinishedEvent(UUID uuid, String wanReplicationName, String wanPublisherId, String mapName,
                                      long durationSecs,
                                      int partitionsSynced, int nodesSynced, long recordsSynced, int minLeafEntryCount,
                                      int maxLeafEntryCount, double avgEntriesPerLeaf, double stdDevEntriesPerLeaf) {
        super(uuid, wanReplicationName, wanPublisherId, mapName, durationSecs, recordsSynced, partitionsSynced);
        this.nodesSynced = nodesSynced;
        this.minLeafEntryCount = minLeafEntryCount;
        this.maxLeafEntryCount = maxLeafEntryCount;
        this.avgEntriesPerLeaf = avgEntriesPerLeaf;
        this.stdDevEntriesPerLeaf = stdDevEntriesPerLeaf;
    }

    @Override
    public EventMetadata.EventType getType() {
        return WAN_SYNC_FINISHED_MERKLE;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("nodesSynced", nodesSynced);
        json.add("minLeafEntryCount", minLeafEntryCount);
        json.add("maxLeafEntryCount", maxLeafEntryCount);
        json.add("avgEntriesPerLeaf", avgEntriesPerLeaf);
        json.add("stdDevEntriesPerLeaf", stdDevEntriesPerLeaf);
        return json;
    }
}
