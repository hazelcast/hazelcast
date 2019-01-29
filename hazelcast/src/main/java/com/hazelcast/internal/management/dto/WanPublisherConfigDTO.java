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

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.internal.management.JsonSerializable;

import java.util.Map;

/**
 * A {@link JsonSerializable} to be used within {@link WanReplicationConfigDTO}
 */
public class WanPublisherConfigDTO implements JsonSerializable {

    private WanPublisherConfig config;

    public WanPublisherConfigDTO(WanPublisherConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject object = new JsonObject();
        object.add("groupName", config.getGroupName());
        object.add("queueCapacity", config.getQueueCapacity());
        object.add("className", config.getClassName());
        object.add("queueFullBehavior", config.getQueueFullBehavior().getId());
        JsonObject properties = new JsonObject();
        for (Map.Entry<String, Comparable> property : config.getProperties().entrySet()) {
            properties.add(property.getKey(), Json.value(property.getValue().toString()));
        }
        object.add("properties", properties);
        return object;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new WanPublisherConfig();
        config.setGroupName(json.get("groupName").asString());
        config.setQueueCapacity(json.get("queueCapacity").asInt());
        config.setClassName(json.get("className").asString());
        int queueFullBehavior = json.get("queueFullBehavior").asInt();
        config.setQueueFullBehavior(WANQueueFullBehavior.getByType(queueFullBehavior));
        JsonObject properties = (JsonObject) json.get("properties");
        Map<String, Comparable> configProperties = config.getProperties();
        for (String propertyName : properties.names()) {
            configProperties.put(propertyName, properties.get(propertyName).asString());
        }
    }

    public WanPublisherConfig getConfig() {
        return config;
    }
}
