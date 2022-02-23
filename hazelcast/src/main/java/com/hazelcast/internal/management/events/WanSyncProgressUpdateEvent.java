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

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_SYNC_PROGRESS_UPDATE;

public class WanSyncProgressUpdateEvent extends AbstractWanAntiEntropyEventBase {
    private final int partitionsToSync;
    private final int partitionsSynced;
    private final int recordsSynced;

    public WanSyncProgressUpdateEvent(UUID uuid, String wanReplicationName, String wanPublisherId, String mapName,
                                      int partitionsToSync, int partitionsSynced, int recordsSynced) {
        super(uuid, wanReplicationName, wanPublisherId, mapName);
        this.partitionsToSync = partitionsToSync;
        this.partitionsSynced = partitionsSynced;
        this.recordsSynced = recordsSynced;
    }

    public int getPartitionsToSync() {
        return partitionsToSync;
    }

    public int getPartitionsSynced() {
        return partitionsSynced;
    }

    public int getRecordsSynced() {
        return recordsSynced;
    }

    @Override
    public EventMetadata.EventType getType() {
        return WAN_SYNC_PROGRESS_UPDATE;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("partitionsToSync", partitionsToSync);
        json.add("partitionsSynced", partitionsSynced);
        json.add("recordsSynced", recordsSynced);
        return json;
    }
}
