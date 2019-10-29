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

import com.hazelcast.config.CustomWanPublisherConfig;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.JsonSerializable;

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

        JsonArray batchPublishers = new JsonArray();
        JsonArray customPublishers = new JsonArray();

        for (WanBatchReplicationPublisherConfig publisherConfig : config.getBatchPublisherConfigs()) {
            batchPublishers.add(new WanBatchReplicationPublisherConfigDTO(publisherConfig).toJson());
        }
        for (CustomWanPublisherConfig publisherConfig : config.getCustomPublisherConfigs()) {
            customPublishers.add(new CustomWanPublisherConfigDTO(publisherConfig).toJson());
        }
        root.add("batchPublishers", batchPublishers);
        root.add("customPublishers", customPublishers);

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

        JsonValue batchPublishers = json.get("batchPublishers");
        if (batchPublishers != null && !batchPublishers.isNull()) {
            for (JsonValue jsonValue : batchPublishers.asArray()) {
                WanBatchReplicationPublisherConfigDTO dto = new WanBatchReplicationPublisherConfigDTO();
                dto.fromJson(jsonValue.asObject());
                config.addWanBatchReplicationPublisherConfig(dto.getConfig());
            }
        }

        JsonValue customPublishers = json.get("customPublishers");
        if (customPublishers != null && !customPublishers.isNull()) {
            for (JsonValue jsonValue : customPublishers.asArray()) {
                CustomWanPublisherConfigDTO dto = new CustomWanPublisherConfigDTO();
                dto.fromJson(jsonValue.asObject());
                config.addCustomPublisherConfig(dto.getConfig());
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
