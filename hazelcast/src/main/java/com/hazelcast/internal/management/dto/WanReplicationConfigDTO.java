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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.JsonSerializable;

import java.util.List;

/**
 * A JSON representation of {@link WanReplicationConfig}.
 */
public class WanReplicationConfigDTO implements JsonSerializable {

    private WanReplicationConfig config;

    public WanReplicationConfigDTO(WanReplicationConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        if (config.getName() != null) {
            root.add("name", config.getName());
        }

        JsonArray publishers = new JsonArray();
        for (WanPublisherConfig publisherConfig : config.getWanPublisherConfigs()) {
            WanPublisherConfigDTO dto = new WanPublisherConfigDTO(publisherConfig);
            publishers.add(dto.toJson());
        }
        root.add("publishers", publishers);

        WanConsumerConfig consumerConfig = config.getWanConsumerConfig();
        if (consumerConfig != null) {
            root.add("consumer", new WanConsumerConfigDTO(consumerConfig).toJson());
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new WanReplicationConfig();

        JsonValue name = json.get("name");
        if (name != null) {
            config.setName(name.asString());
        }

        JsonValue publishers = json.get("publishers");
        if (publishers != null && !publishers.isNull()) {
            List<WanPublisherConfig> publisherConfigs = config.getWanPublisherConfigs();
            for (JsonValue publisher : publishers.asArray()) {
                WanPublisherConfigDTO publisherDTO = new WanPublisherConfigDTO();
                publisherDTO.fromJson(publisher.asObject());
                publisherConfigs.add(publisherDTO.getConfig());
            }
        }

        JsonValue consumer = json.get("consumer");
        if (consumer != null && !consumer.isNull()) {
            WanConsumerConfigDTO consumerDTO = new WanConsumerConfigDTO();
            consumerDTO.fromJson(consumer.asObject());
            config.setWanConsumerConfig(consumerDTO.getConfig());
        }
    }

    public WanReplicationConfig getConfig() {
        return config;
    }
}
