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

abstract class AbstractWanEventBase extends AbstractEventBase {
    protected final String wanReplicationName;
    protected final String wanPublisherId;
    protected final String mapName;

    AbstractWanEventBase(String wanReplicationName, String wanPublisherId, String mapName) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.mapName = mapName;
    }

    public String getWanReplicationName() {
        return wanReplicationName;
    }

    public String getWanPublisherId() {
        return wanPublisherId;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanReplicationName", wanReplicationName);
        json.add("targetGroupName", wanPublisherId);
        json.add("mapName", mapName);
        return json;
    }
}
