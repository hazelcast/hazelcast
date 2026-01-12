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

package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.events.EventMetadata.EventType;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_EVENT_DROPPED;

public class WanReplicationEventDroppedEvent extends AbstractEventBase {
    private final String wanReplicationName;
    private final String wanEventObjectName;
    private final String wanPublisherId;

    public WanReplicationEventDroppedEvent(String wanReplicationName, String wanEventObjectName, String wanPublisherId) {
        this.wanReplicationName = wanReplicationName;
        this.wanEventObjectName = wanEventObjectName;
        this.wanPublisherId = wanPublisherId;
    }

    @Override
    public EventType getType() {
        return WAN_EVENT_DROPPED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanReplicationName", wanReplicationName);
        json.add("wanEventObjectName", wanEventObjectName);
        json.add("wanPublisherId", wanPublisherId);
        return json;
    }
}
