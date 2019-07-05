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

package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;

import java.util.UUID;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_CONSISTENCY_CHECK_FINISHED;

public class WanConsistencyCheckFinishedEvent extends AbstractWanAntiEntropyEventBase {
    private final int diffCount;
    private final int checkedCount;
    private final int entriesToSync;

    public WanConsistencyCheckFinishedEvent(UUID uuid, String wanReplicationName, String targetGroupName, String mapName,
                                            int diffCount, int checkedCount, int entriesToSync) {
        super(uuid, wanReplicationName, targetGroupName, mapName);

        this.diffCount = diffCount;
        this.checkedCount = checkedCount;
        this.entriesToSync = entriesToSync;
    }

    @Override
    public EventMetadata.EventType getType() {
        return WAN_CONSISTENCY_CHECK_FINISHED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = super.toJson();
        json.add("diffCount", diffCount);
        json.add("checkedCount", checkedCount);
        json.add("entriesToSync", entriesToSync);
        return json;
    }
}
