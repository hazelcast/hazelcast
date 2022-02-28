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

import com.hazelcast.internal.management.events.EventMetadata.EventType;

import java.util.UUID;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_SYNC_STARTED;

public class WanSyncStartedEvent extends AbstractWanAntiEntropyEventBase {
    public WanSyncStartedEvent(UUID uuid, String wanReplicationName, String wanPublisherId, String mapName) {
        super(uuid, wanReplicationName, wanPublisherId, mapName);
    }

    @Override
    public EventType getType() {
        return WAN_SYNC_STARTED;
    }
}
