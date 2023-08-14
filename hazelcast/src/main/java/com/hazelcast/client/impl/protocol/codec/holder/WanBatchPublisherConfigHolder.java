/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.internal.serialization.Data;

import java.util.Map;

public final class WanBatchPublisherConfigHolder {
    private final String publisherId;
    private final String className;
    private final Data implementation;
    private final Map<String, Data> properties;
    private final String clusterName;
    private final boolean snapshotEnabled;
    private final byte initialPublisherState;
    private final int queueCapacity;
    private final int batchSize;
    private final int batchMaxDelayMillis;
    private final int responseTimeoutMillis;
    private final int queueFullBehavior;
    private final int acknowledgeType;
    private final int discoveryPeriodSeconds;
    private final int maxTargetEndpoints;
    private final int maxConcurrentInvocations;
    private final boolean useEndpointPrivateAddress;
    private final long idleMinParkNs;
    private final long idleMaxParkNs;
    private final String targetEndpoints;
    private final AwsConfig awsConfig;
    private final GcpConfig gcpConfig;
    private final AzureConfig azureConfig;
    private final KubernetesConfig kubernetesConfig;
    private final EurekaConfig eurekaConfig;
    private final DiscoveryConfigHolder discoveryConfig;
    private final byte syncConfig;
    private final String endpoint;

    @SuppressWarnings("checkstyle:executablestatementcount")
    public WanBatchPublisherConfigHolder(String publisherId, String className, Data implementation, Map<String, Data> properties,
                                         String clusterName, boolean snapshotEnabled, byte initialPublisherState,
                                         int queueCapacity, int batchSize, int batchMaxDelayMillis, int responseTimeoutMillis,
                                         int queueFullBehavior, int acknowledgeType, int discoveryPeriodSeconds,
                                         int maxTargetEndpoints, int maxConcurrentInvocations, boolean useEndpointPrivateAddress,
                                         long idleMinParkNs, long idleMaxParkNs, String targetEndpoints, AwsConfig awsConfig,
                                         GcpConfig gcpConfig, AzureConfig azureConfig, KubernetesConfig kubernetesConfig,
                                         EurekaConfig eurekaConfig, DiscoveryConfigHolder discoveryConfig, byte syncConfig,
                                         String endpoint) {
        this.publisherId = publisherId;
        this.className = className;
        this.implementation = implementation;
        this.properties = properties;
        this.clusterName = clusterName;
        this.snapshotEnabled = snapshotEnabled;
        this.initialPublisherState = initialPublisherState;
        this.queueCapacity = queueCapacity;
        this.batchSize = batchSize;
        this.batchMaxDelayMillis = batchMaxDelayMillis;
        this.responseTimeoutMillis = responseTimeoutMillis;
        this.queueFullBehavior = queueFullBehavior;
        this.acknowledgeType = acknowledgeType;
        this.discoveryPeriodSeconds = discoveryPeriodSeconds;
        this.maxTargetEndpoints = maxTargetEndpoints;
        this.maxConcurrentInvocations = maxConcurrentInvocations;
        this.useEndpointPrivateAddress = useEndpointPrivateAddress;
        this.idleMinParkNs = idleMinParkNs;
        this.idleMaxParkNs = idleMaxParkNs;
        this.targetEndpoints = targetEndpoints;
        this.awsConfig = awsConfig;
        this.gcpConfig = gcpConfig;
        this.azureConfig = azureConfig;
        this.kubernetesConfig = kubernetesConfig;
        this.eurekaConfig = eurekaConfig;
        this.discoveryConfig = discoveryConfig;
        this.syncConfig = syncConfig;
        this.endpoint = endpoint;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    public byte getInitialPublisherState() {
        return initialPublisherState;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    public int getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    public int getQueueFullBehavior() {
        return queueFullBehavior;
    }

    public int getAcknowledgeType() {
        return acknowledgeType;
    }

    public int getDiscoveryPeriodSeconds() {
        return discoveryPeriodSeconds;
    }

    public int getMaxTargetEndpoints() {
        return maxTargetEndpoints;
    }

    public int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    public boolean isUseEndpointPrivateAddress() {
        return useEndpointPrivateAddress;
    }

    public long getIdleMinParkNs() {
        return idleMinParkNs;
    }

    public long getIdleMaxParkNs() {
        return idleMaxParkNs;
    }

    public String getTargetEndpoints() {
        return targetEndpoints;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public GcpConfig getGcpConfig() {
        return gcpConfig;
    }

    public AzureConfig getAzureConfig() {
        return azureConfig;
    }

    public KubernetesConfig getKubernetesConfig() {
        return kubernetesConfig;
    }

    public EurekaConfig getEurekaConfig() {
        return eurekaConfig;
    }

    public DiscoveryConfigHolder getDiscoveryConfig() {
        return discoveryConfig;
    }

    public byte getSyncConfig() {
        return syncConfig;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getPublisherId() {
        return publisherId;
    }

    public String getClassName() {
        return className;
    }

    public Data getImplementation() {
        return implementation;
    }

    public Map<String, Data> getProperties() {
        return properties;
    }
}
