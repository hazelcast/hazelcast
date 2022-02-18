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
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;
import com.hazelcast.wan.WanPublisherState;

import java.util.function.Consumer;

import static com.hazelcast.internal.util.JsonUtil.fromJsonObject;
import static com.hazelcast.internal.util.JsonUtil.toJsonObject;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of {@link WanBatchPublisherConfig}.
 */
public class WanBatchPublisherConfigDTO implements JsonSerializable {

    private WanBatchPublisherConfig config;

    public WanBatchPublisherConfigDTO() {
    }

    public WanBatchPublisherConfigDTO(WanBatchPublisherConfig config) {
        this.config = config;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public JsonObject toJson() {
        JsonObject root = new JsonObject();

        root.add("clusterName", config.getClusterName());

        if (config.getPublisherId() != null) {
            root.add("publisherId", config.getPublisherId());
        }

        root.add("batchSize", config.getBatchSize());
        root.add("batchMaxDelayMillis", config.getBatchMaxDelayMillis());
        root.add("responseTimeoutMillis", config.getResponseTimeoutMillis());
        if (config.getAcknowledgeType() != null) {
            root.add("acknowledgeType", config.getAcknowledgeType().getId());
        }
        if (config.getInitialPublisherState() != null) {
            root.add("initialPublisherState", config.getInitialPublisherState().getId());
        }
        root.add("snapshotEnabled", config.isSnapshotEnabled());
        root.add("idleMaxParkNs", config.getIdleMaxParkNs());
        root.add("idleMinParkNs", config.getIdleMinParkNs());
        root.add("maxConcurrentInvocations", config.getMaxConcurrentInvocations());
        root.add("discoveryPeriodSeconds", config.getDiscoveryPeriodSeconds());
        root.add("useEndpointPrivateAddress", config.isUseEndpointPrivateAddress());
        if (config.getQueueFullBehavior() != null) {
            root.add("queueFullBehavior", config.getQueueFullBehavior().getId());
        }
        root.add("maxTargetEndpoints", config.getMaxTargetEndpoints());
        root.add("queueCapacity", config.getQueueCapacity());

        if (config.getTargetEndpoints() != null) {
            root.add("targetEndpoints", config.getTargetEndpoints());
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

        WanSyncConfig syncConfig = config.getSyncConfig();
        if (syncConfig != null) {
            root.add("sync", new WanSyncConfigDTO(syncConfig).toJson());
        }

        if (config.getEndpoint() != null) {
            root.add("endpoint", config.getEndpoint());
        }

        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }
        return root;
    }

    @Override
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public void fromJson(JsonObject json) {
        config = new WanBatchPublisherConfig();

        consumeIfExists(json, "clusterName", v -> config.setClusterName(v.asString()));
        consumeIfExists(json, "publisherId", v -> config.setPublisherId(v.asString()));
        consumeIfExists(json, "batchSize", v -> config.setBatchSize(v.asInt()));
        consumeIfExists(json, "batchMaxDelayMillis", v -> config.setBatchMaxDelayMillis(v.asInt()));
        consumeIfExists(json, "responseTimeoutMillis", v -> config.setResponseTimeoutMillis(v.asInt()));
        consumeIfExists(json, "acknowledgeType", v -> config.setAcknowledgeType(WanAcknowledgeType.getById(v.asInt())));
        consumeIfExists(json, "initialPublisherState",
                v -> config.setInitialPublisherState(WanPublisherState.getByType((byte) v.asInt())));
        consumeIfExists(json, "snapshotEnabled", v -> config.setSnapshotEnabled(v.asBoolean()));
        consumeIfExists(json, "idleMaxParkNs", v -> config.setIdleMaxParkNs(v.asLong()));
        consumeIfExists(json, "idleMinParkNs", v -> config.setIdleMinParkNs(v.asLong()));
        consumeIfExists(json, "maxConcurrentInvocations", v -> config.setMaxConcurrentInvocations(v.asInt()));
        consumeIfExists(json, "discoveryPeriodSeconds", v -> config.setDiscoveryPeriodSeconds(v.asInt()));
        consumeIfExists(json, "useEndpointPrivateAddress", v -> config.setUseEndpointPrivateAddress(v.asBoolean()));
        consumeIfExists(json, "queueFullBehavior", v -> config.setQueueFullBehavior(WanQueueFullBehavior.getByType(v.asInt())));
        consumeIfExists(json, "maxTargetEndpoints", v -> config.setMaxTargetEndpoints(v.asInt()));
        consumeIfExists(json, "queueCapacity", v -> config.setQueueCapacity(v.asInt()));
        consumeIfExists(json, "targetEndpoints", v -> config.setTargetEndpoints(v.asString()));

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
            config.setSyncConfig(syncDTO.getConfig());
        }

        consumeIfExists(json, "endpoint", v -> config.setEndpoint(v.asString()));

        config.setProperties(fromJsonObject((JsonObject) json.get("properties")));
    }

    private void consumeIfExists(JsonObject json, String attribute, Consumer<JsonValue> valueConsumer) {
        JsonValue value = json.get(attribute);
        if (value != null && !value.isNull()) {
            valueConsumer.accept(value);
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

    public WanBatchPublisherConfig getConfig() {
        return config;
    }
}
