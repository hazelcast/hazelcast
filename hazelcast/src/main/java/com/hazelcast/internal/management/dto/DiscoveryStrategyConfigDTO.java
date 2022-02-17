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

import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.fromJsonObject;
import static com.hazelcast.internal.util.JsonUtil.toJsonObject;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of {@link com.hazelcast.config.DiscoveryStrategyConfig}.
 */
public class DiscoveryStrategyConfigDTO implements JsonSerializable {

    private DiscoveryStrategyConfig config;

    public DiscoveryStrategyConfigDTO() {
    }

    public DiscoveryStrategyConfigDTO(DiscoveryStrategyConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();

        if (config.getClassName() != null) {
            root.add("className", config.getClassName());
        } else if (config.getDiscoveryStrategyFactory() != null) {
            root.add("className", config.getDiscoveryStrategyFactory().getClass().getName());
        }
        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new DiscoveryStrategyConfig();

        JsonValue className = json.get("className");
        if (className != null && !className.isNull()) {
            config.setClassName(className.asString());
        }

        config.setProperties(fromJsonObject((JsonObject) json.get("properties")));
    }

    public DiscoveryStrategyConfig getConfig() {
        return config;
    }
}
