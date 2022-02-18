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

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.util.JsonUtil.fromJsonObject;
import static com.hazelcast.internal.util.JsonUtil.toJsonObject;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of a
 * {@link com.hazelcast.config.AliasedDiscoveryConfig} implementation.
 *
 */
public class AliasedDiscoveryConfigDTO implements JsonSerializable {

    private String tag;
    private AliasedDiscoveryConfig config;

    public AliasedDiscoveryConfigDTO(String tag) {
        this.tag = tag;
    }

    public AliasedDiscoveryConfigDTO(AliasedDiscoveryConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("enabled", config.isEnabled())
                .add("usePublicIp", config.isUsePublicIp());

        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = AliasedDiscoveryConfigUtils.newConfigFor(tag);

        JsonValue enabled = json.get("enabled");
        if (enabled != null && !enabled.isNull()) {
            config.setEnabled(enabled.asBoolean());
        }

        JsonValue usePublicIp = json.get("usePublicIp");
        if (usePublicIp != null && !usePublicIp.isNull()) {
            config.setUsePublicIp(usePublicIp.asBoolean());
        }

        Map<String, Comparable> properties = fromJsonObject((JsonObject) json.get("properties"));
        for (Entry<String, Comparable> property : properties.entrySet()) {
            config.setProperty(property.getKey(), (String) property.getValue());
        }
    }

    public AliasedDiscoveryConfig getConfig() {
        return config;
    }
}
