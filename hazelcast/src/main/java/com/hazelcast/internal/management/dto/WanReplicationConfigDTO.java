/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;

import java.util.List;

/**
 * A {@link JsonSerializable} DTO to add new {@link com.hazelcast.config.WanReplicationConfig} objects from
 * management center
 */
public class WanReplicationConfigDTO implements JsonSerializable {

    private WanReplicationConfig config;

    public WanReplicationConfigDTO(WanReplicationConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("name", config.getName());
        JsonArray publisherList = new JsonArray();
        for (WanPublisherConfig publisherConfig : config.getWanPublisherConfigs()) {
            WanPublisherConfigDTO dto = new WanPublisherConfigDTO(publisherConfig);
            publisherList.add(dto.toJson());
        }
        root.add("publishers", publisherList);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new WanReplicationConfig();
        config.setName(json.get("name").asString());
        List<WanPublisherConfig> publisherConfigs = config.getWanPublisherConfigs();
        JsonArray publishers = json.get("publishers").asArray();
        int size = publishers.size();
        for (int i = 0; i < size; i++) {
            WanPublisherConfigDTO dto = new WanPublisherConfigDTO(new WanPublisherConfig());
            dto.fromJson(publishers.get(0).asObject());
            publisherConfigs.add(dto.getConfig());
        }
    }

    public WanReplicationConfig getConfig() {
        return config;
    }
}
