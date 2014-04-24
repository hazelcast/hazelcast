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

package com.hazelcast.management.request;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.MapConfigAdapter;
import com.hazelcast.management.operation.GetMapConfigOperation;
import com.hazelcast.management.operation.UpdateMapConfigOperation;
import java.util.Set;

public class MapConfigRequest implements ConsoleRequest {

    private String mapName;
    private MapConfigAdapter config;
    private boolean update;

    public MapConfigRequest() {
    }

    public MapConfigRequest(String mapName, MapConfigAdapter config, boolean update) {
        this.mapName = mapName;
        this.config = config;
        this.update = update;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_CONFIG;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        final JsonObject result = new JsonObject();
        result.add("update", update);
        if (update) {
            final Set<Member> members = mcs.getHazelcastInstance().getCluster().getMembers();
            for (Member member : members) {
                mcs.callOnMember(member, new UpdateMapConfigOperation(mapName, config.getMapConfig()));
            }
            result.add("updateResult", "success");
        } else {
            MapConfig cfg = (MapConfig) mcs.callOnThis(new GetMapConfigOperation(mapName));
            if (cfg != null) {
                result.add("hasMapConfig", true);
                result.add("mapConfig", new MapConfigAdapter(cfg).toJson());
            } else {
                result.add("hasMapConfig", false);
            }
        }
        root.add("result", result);
    }

    @Override
    public Object readResponse(JsonObject in) {
        update = in.get("update").asBoolean();
        if (!update) {
            boolean hasMapConfig = in.get("hasMapConfig").asBoolean();
            if (hasMapConfig) {
                final MapConfigAdapter adapter = new MapConfigAdapter();
                adapter.fromJson(in.get("mapConfig").asObject());
                return adapter.getMapConfig();
            } else {
                return null;
            }
        }
        return in.get("updateResult").asString();
    }

    @Override
    public JsonValue toJson() {
        JsonObject root = new JsonObject();
        root.add("mapName", mapName);
        root.add("update", update);
        root.add("config", config.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        mapName = json.get("mapName").asString();
        update = json.get("update").asBoolean();
        config = new MapConfigAdapter();
        config.fromJson(json.get("config").asObject());
    }
}
