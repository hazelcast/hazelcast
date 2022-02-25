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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.Collection;

/**
 * A JSON representation of {@link DiscoveryConfig}.
 */
public class DiscoveryConfigDTO implements JsonSerializable {

    private DiscoveryConfig config;

    public DiscoveryConfigDTO() {
    }

    public DiscoveryConfigDTO(DiscoveryConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("nodeFilterClass", config.getNodeFilterClass());

        JsonArray strategies = new JsonArray();
        for (DiscoveryStrategyConfig strategyConfig : config.getDiscoveryStrategyConfigs()) {
            DiscoveryStrategyConfigDTO dto = new DiscoveryStrategyConfigDTO(strategyConfig);
            strategies.add(dto.toJson());
        }
        root.add("discoveryStrategy", strategies);

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new DiscoveryConfig();

        JsonValue nodeFilterClass = json.get("nodeFilterClass");
        if (nodeFilterClass != null && !nodeFilterClass.isNull()) {
            config.setNodeFilterClass(nodeFilterClass.asString());
        }

        JsonValue discoveryStrategies = json.get("discoveryStrategy");
        if (discoveryStrategies != null && !discoveryStrategies.isNull()) {
            Collection<DiscoveryStrategyConfig> strategyConfigs = config.getDiscoveryStrategyConfigs();
            for (JsonValue strategy : discoveryStrategies.asArray()) {
                DiscoveryStrategyConfigDTO strategyDTO = new DiscoveryStrategyConfigDTO();
                strategyDTO.fromJson(strategy.asObject());
                strategyConfigs.add(strategyDTO.getConfig());
            }
        }
    }

    public DiscoveryConfig getConfig() {
        return config;
    }
}
