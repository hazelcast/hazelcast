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

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.JsonSerializable;

import static com.hazelcast.util.JsonUtil.fromJsonObject;
import static com.hazelcast.util.JsonUtil.toJsonObject;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of {@link WanPublisherConfig}.
 */
public class WanPublisherConfigDTO implements JsonSerializable {

    private WanPublisherConfig config;

    public WanPublisherConfigDTO() {
    }

    public WanPublisherConfigDTO(WanPublisherConfig config) {
        this.config = config;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public JsonObject toJson() {
        JsonObject root = new JsonObject();

        if (config.getGroupName() != null) {
            root.add("groupName", config.getGroupName());
        }

        if (config.getPublisherId() != null) {
            root.add("publisherId", config.getPublisherId());
        }

        root.add("queueCapacity", config.getQueueCapacity());

        if (config.getQueueFullBehavior() != null) {
            root.add("queueFullBehavior", config.getQueueFullBehavior().getId());
        }
        if (config.getInitialPublisherState() != null) {
            root.add("initialPublisherState", config.getInitialPublisherState().getId());
        }

        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }

        if (config.getClassName() != null) {
            root.add("className", config.getClassName());
        }

        serializeAliasedDiscoveryConfig(root, "aws", config.getAwsConfig());
        serializeAliasedDiscoveryConfig(root, "gcp", config.getGcpConfig());
        serializeAliasedDiscoveryConfig(root, "azure", config.getAzureConfig());
        serializeAliasedDiscoveryConfig(root, "kubernetes", config.getKubernetesConfig());
        serializeAliasedDiscoveryConfig(root, "eureka", config.getEurekaConfig());

        DiscoveryConfig discoveryConfig = config.getDiscoveryConfig();
        if (discoveryConfig != null) {
            root.add("discovery", new DiscoveryConfigDTO(discoveryConfig).toJson());
        }

        WanSyncConfig syncConfig = config.getWanSyncConfig();
        if (syncConfig != null) {
            root.add("sync", new WanSyncConfigDTO(syncConfig).toJson());
        }
        return root;
    }

    @Override
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public void fromJson(JsonObject json) {
        config = new WanPublisherConfig();
        JsonValue groupName = json.get("groupName");
        if (groupName != null && !groupName.isNull()) {
            config.setGroupName(groupName.asString());
        }

        JsonValue publisherId = json.get("publisherId");
        if (publisherId != null && !publisherId.isNull()) {
            config.setPublisherId(publisherId.asString());
        }

        JsonValue queueCapacity = json.get("queueCapacity");
        if (queueCapacity != null && !queueCapacity.isNull()) {
            config.setQueueCapacity(queueCapacity.asInt());
        }

        JsonValue queueFullBehavior = json.get("queueFullBehavior");
        if (queueFullBehavior != null && !queueFullBehavior.isNull()) {
            config.setQueueFullBehavior(
                    WANQueueFullBehavior.getByType(queueFullBehavior.asInt()));
        }

        JsonValue initialPublisherState = json.get("initialPublisherState");
        if (initialPublisherState != null && !initialPublisherState.isNull()) {
            config.setInitialPublisherState(
                    WanPublisherState.getByType((byte) initialPublisherState.asInt()));
        }

        config.setProperties(fromJsonObject((JsonObject) json.get("properties")));

        JsonValue className = json.get("className");
        if (className != null && !className.isNull()) {
            config.setClassName(className.asString());
        }

        AwsConfig awsConfig = (AwsConfig) this.deserializeAliasedDiscoveryConfig(json, "aws");
        if (awsConfig != null) {
            config.setAwsConfig(awsConfig);
        }

        GcpConfig gcpConfig = (GcpConfig) this.deserializeAliasedDiscoveryConfig(json, "gcp");
        if (gcpConfig != null) {
            config.setGcpConfig(gcpConfig);
        }
        AzureConfig azureConfig = (AzureConfig) this.deserializeAliasedDiscoveryConfig(json, "azure");
        if (azureConfig != null) {
            config.setAzureConfig(azureConfig);
        }
        KubernetesConfig kubernetesConfig = (KubernetesConfig) this.deserializeAliasedDiscoveryConfig(json, "kubernetes");
        if (kubernetesConfig != null) {
            config.setKubernetesConfig(kubernetesConfig);
        }
        EurekaConfig eurekaConfig = (EurekaConfig) this.deserializeAliasedDiscoveryConfig(json, "eureka");
        if (eurekaConfig != null) {
            config.setEurekaConfig(eurekaConfig);
        }

        JsonValue discoveryJson = json.get("discovery");
        if (discoveryJson != null && !discoveryJson.isNull()) {
            DiscoveryConfigDTO discoveryDTO = new DiscoveryConfigDTO();
            discoveryDTO.fromJson(discoveryJson.asObject());
            config.setDiscoveryConfig(discoveryDTO.getConfig());
        }

        JsonValue syncJson = json.get("sync");
        if (syncJson != null && !syncJson.isNull()) {
            WanSyncConfigDTO syncDTO = new WanSyncConfigDTO();
            syncDTO.fromJson(syncJson.asObject());
            config.setWanSyncConfig(syncDTO.getConfig());
        }
    }

    /**
     * Deserializes the aliased discovery config nested under the {@code tag} in the provided JSON.
     *
     * @param json the JSON object containing the serialized config
     * @param tag  the tag under which the config is nested
     * @return the deserialized config or {@code null} if the serialized config
     * was missing in the JSON object
     */
    private AliasedDiscoveryConfig deserializeAliasedDiscoveryConfig(
            JsonObject json, String tag) {
        JsonValue configJson = json.get(tag);
        if (configJson != null && !configJson.isNull()) {
            AliasedDiscoveryConfigDTO dto = new AliasedDiscoveryConfigDTO(tag);
            dto.fromJson(configJson.asObject());
            return dto.getConfig();
        }
        return null;
    }

    /**
     * Serializes the provided aliased discovery config and adds the serialized
     * contents under the tag in the JSON object if the config is not
     * {@code null}.
     *
     * @param object the JSON object to which the config is added
     * @param tag    the tag under which the config is added
     * @param config the configuration
     */
    private void serializeAliasedDiscoveryConfig(
            JsonObject object, String tag, AliasedDiscoveryConfig config) {
        if (config != null) {
            object.add(tag, new AliasedDiscoveryConfigDTO(config).toJson());
        }
    }

    public WanPublisherConfig getConfig() {
        return config;
    }
}
