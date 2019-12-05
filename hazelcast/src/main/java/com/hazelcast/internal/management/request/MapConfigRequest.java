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

package com.hazelcast.internal.management.request;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.MapConfigDTO;
import com.hazelcast.internal.management.operation.GetMapConfigOperation;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;

import java.util.Set;

import static com.hazelcast.internal.management.ManagementCenterService.resolveFuture;
import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * Request for updating map configuration from Management Center.
 */
public class MapConfigRequest implements ConsoleRequest {

    private String mapName;
    private MapConfigDTO mapConfigDTO;
    private boolean update;

    public MapConfigRequest() {
    }

    public MapConfigRequest(String mapName, MapConfigDTO mapConfigDTO, boolean update) {
        this.mapName = mapName;
        this.mapConfigDTO = mapConfigDTO;
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
                UpdateMapConfigOperation operation = new UpdateMapConfigOperation(
                        mapName,
                        mapConfigDTO.getConfig().getTimeToLiveSeconds(),
                        mapConfigDTO.getConfig().getMaxIdleSeconds(),
                        mapConfigDTO.getConfig().getEvictionConfig().getSize(),
                        mapConfigDTO.getConfig().getEvictionConfig().getMaxSizePolicy().getId(),
                        mapConfigDTO.getConfig().isReadBackupData(),
                        mapConfigDTO.getConfig().getEvictionConfig().getEvictionPolicy().getId());
                resolveFuture(mcs.callOnMember(member, operation));
            }
            result.add("updateResult", "success");
        } else {
            MapConfig cfg = (MapConfig) resolveFuture(mcs.callOnThis(new GetMapConfigOperation(mapName)));
            if (cfg != null) {
                result.add("hasMapConfig", true);
                result.add("mapConfig", new MapConfigDTO(cfg).toJson());
            } else {
                result.add("hasMapConfig", false);
            }
        }
        root.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
        mapName = getString(json, "mapName");
        update = getBoolean(json, "update");
        mapConfigDTO = new MapConfigDTO();
        mapConfigDTO.fromJson(getObject(json, "config"));
    }
}
