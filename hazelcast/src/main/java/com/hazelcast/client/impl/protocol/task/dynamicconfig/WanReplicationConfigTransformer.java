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
package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.codec.holder.DiscoveryConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.DiscoveryStrategyConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanSyncConfigHolder;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.wan.WanConsumer;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.WanPublisherState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Transformer for mapping between the API holder types {@link WanBatchPublisherConfigHolder},
 * {@link WanCustomPublisherConfigHolder} and {@link WanConsumerConfigHolder}, and their respective non-holder correspondents
 * {@link WanBatchPublisherConfig}, {@link WanCustomPublisherConfig} and {@link WanConsumerConfig}.
 */
public final class WanReplicationConfigTransformer {
    private final SerializationService serializationService;

    public WanReplicationConfigTransformer(SerializationService serializationService) {
        this.serializationService = serializationService;
    }


    @Nullable
    public WanConsumerConfigHolder toHolder(@Nullable WanConsumerConfig config) {
        if (config == null) {
            return null;
        }
        return new WanConsumerConfigHolder(
                config.isPersistWanReplicatedData(),
                config.getClassName(),
                serializationService.toData(config.getImplementation()),
                toHolderProperties(config.getProperties()));
    }

    @Nonnull
    public WanCustomPublisherConfigHolder toHolder(@Nonnull WanCustomPublisherConfig config) {
        checkNotNull(config, "WAN custom publisher config must be provided");
        return new WanCustomPublisherConfigHolder(
                config.getPublisherId(),
                config.getClassName(),
                serializationService.toData(config.getImplementation()),
                toHolderProperties(config.getProperties()));
    }

    @Nonnull
    public Map<String, Data> toHolderProperties(@Nullable Map<String, Comparable> m) {
        if (m == null) {
            return Collections.emptyMap();
        }

        Map<String, Data> safeMapping = new HashMap<>();
        for (Map.Entry<String, Comparable> e : m.entrySet()) {
            safeMapping.put(e.getKey(), serializationService.toData(e.getValue()));
        }
        return safeMapping;
    }

    @Nonnull
    public WanBatchPublisherConfigHolder toHolder(@Nonnull WanBatchPublisherConfig config) {
        checkNotNull(config, "WAN batch publisher config must be provided");
        return new WanBatchPublisherConfigHolder(
                config.getPublisherId(),
                config.getClassName(),
                serializationService.toData(config.getImplementation()),
                toHolderProperties(config.getProperties()),
                config.getClusterName(),
                config.isSnapshotEnabled(),
                config.getInitialPublisherState().getId(),
                config.getQueueCapacity(),
                config.getBatchSize(),
                config.getBatchMaxDelayMillis(),
                config.getResponseTimeoutMillis(),
                config.getQueueFullBehavior().getId(),
                config.getAcknowledgeType().getId(),
                config.getDiscoveryPeriodSeconds(),
                config.getMaxTargetEndpoints(),
                config.getMaxConcurrentInvocations(),
                config.isUseEndpointPrivateAddress(),
                config.getIdleMinParkNs(),
                config.getIdleMaxParkNs(),
                config.getTargetEndpoints(),
                config.getAwsConfig(),
                config.getGcpConfig(),
                config.getAzureConfig(),
                config.getKubernetesConfig(),
                config.getEurekaConfig(),
                toHolder(config.getDiscoveryConfig()),
                toHolder(config.getSyncConfig()),
                config.getEndpoint()
        );
    }

    @Nonnull
    Map<String, Comparable> toProperties(@Nullable Map<String, Data> properties) {
        if (properties == null) {
            return Collections.emptyMap();
        }

        Map<String, Comparable> requiredProperties = new HashMap<>();
        for (Map.Entry<String, Data> e : properties.entrySet()) {
            Comparable value = serializationService.toObject(e.getValue());
            // we don't check the [value] if it's null
            requiredProperties.put(e.getKey(), value);
        }
        return requiredProperties;
    }

    @Nullable
    WanConsumerConfig toConfig(@Nullable WanConsumerConfigHolder holder) {
        if (holder == null) {
            return null;
        }

        WanConsumerConfig config = new WanConsumerConfig();
        config.setProperties(toProperties(holder.getProperties()));
        config.setPersistWanReplicatedData(holder.isPersistWanReplicatedData());

        // note. class name and implementation are mutually exclusive -- className seems the safer of the two
        boolean classNameProvided = holder.getClassName() != null;
        if (classNameProvided) {
            config.setClassName(holder.getClassName());
        }
        if (!classNameProvided && holder.getImplementation() != null) {
            WanConsumer wanConsumer = serializationService.toObject(holder.getImplementation());
            config.setImplementation(wanConsumer);
        }
        return config;
    }

    @Nonnull
    WanCustomPublisherConfig toConfig(@Nonnull WanCustomPublisherConfigHolder holder) {
        checkNotNull(holder, "WAN custom publisher config holder must be provided");
        WanCustomPublisherConfig config = new WanCustomPublisherConfig();
        config.setPublisherId(holder.getPublisherId());
        config.setProperties(toProperties(holder.getProperties()));

        // note. class name and implementation are mutually exclusive -- className seems the safer of the two
        boolean classNameProvided = holder.getClassName() != null;
        if (classNameProvided) {
            config.setClassName(holder.getClassName());
        }
        if (!classNameProvided && holder.getImplementation() != null) {
            WanPublisher wanPublisher = serializationService.toObject(holder.getImplementation());
            config.setImplementation(wanPublisher);
        }
        return config;
    }

    @Nonnull
    WanPublisherState getWanPublisherState(byte id) {
        switch (id) {
            case 0:
                return WanPublisherState.REPLICATING;
            case 1:
                return WanPublisherState.PAUSED;
            case 2:
                return WanPublisherState.STOPPED;
            default:
                return WanBatchPublisherConfig.DEFAULT_INITIAL_PUBLISHER_STATE;
        }
    }

    WanQueueFullBehavior getWanQueueFullBehaviour(int id) {
        switch (id) {
            case 0:
                return WanQueueFullBehavior.DISCARD_AFTER_MUTATION;
            case 1:
                return WanQueueFullBehavior.THROW_EXCEPTION;
            case 2:
                return WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE;
            default:
                return WanBatchPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR;
        }
    }

    @Nonnull
    WanAcknowledgeType getWanAcknowledgeType(int id) {
        switch (id) {
            case 0:
                return WanAcknowledgeType.ACK_ON_RECEIPT;
            case 1:
                return WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
            default:
                return WanBatchPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE;
        }
    }

    @Nonnull
    DiscoveryStrategyConfigHolder toHolder(@Nonnull DiscoveryStrategyConfig config) {
        Map<String, Data> properties = toHolderProperties(config.getProperties());
        return new DiscoveryStrategyConfigHolder(config.getClassName(), properties);
    }

    DiscoveryStrategyConfig toConfig(DiscoveryStrategyConfigHolder holder) {
        Map<String, Comparable> properties = toProperties(holder.getProperties());
        return new DiscoveryStrategyConfig(holder.getClassName(), properties);
    }

    DiscoveryConfigHolder toHolder(@Nonnull DiscoveryConfig config) {
        List<DiscoveryStrategyConfigHolder> discoStrategies =
                config.getDiscoveryStrategyConfigs().stream()
                      .filter(Objects::nonNull)
                      .map(this::toHolder)
                      .collect(Collectors.toList());
        Data discoveryServiceProvider = serializationService.toData(config.getDiscoveryServiceProvider());
        Data nodeFilter = serializationService.toData(config.getNodeFilter());
        return new DiscoveryConfigHolder(discoStrategies, discoveryServiceProvider, nodeFilter, config.getNodeFilterClass());
    }

    @Nonnull
    DiscoveryConfig toConfig(@Nonnull DiscoveryConfigHolder holder) {
        List<DiscoveryStrategyConfig> discoveryStrategyConfigs =
                holder.getDiscoveryStrategyConfigs().stream()
                        .filter(Objects::nonNull)
                        .map(this::toConfig)
                        .collect(Collectors.toList());
        DiscoveryServiceProvider discoveryServiceProvider = serializationService.toObject(holder.getDiscoveryServiceProvider());
        NodeFilter nodeFilter = serializationService.toObject(holder.getNodeFilter());
        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.setDiscoveryStrategyConfigs(discoveryStrategyConfigs);
        discoveryConfig.setDiscoveryServiceProvider(discoveryServiceProvider);
        if (holder.getNodeFilterClass() != null) {
            discoveryConfig.setNodeFilterClass(holder.getNodeFilterClass());
        }
        if (nodeFilter != null) {
            discoveryConfig.setNodeFilter(nodeFilter);
        }
        return discoveryConfig;
    }

    @Nonnull
    WanSyncConfig toConfig(@Nonnull WanSyncConfigHolder holder) {
        ConsistencyCheckStrategy strategy = ConsistencyCheckStrategy.getById(holder.getConsistencyCheckStrategy());
        WanSyncConfig config = new WanSyncConfig();
        config.setConsistencyCheckStrategy(strategy);
        return config;
    }

    @Nonnull
    WanSyncConfigHolder toHolder(WanSyncConfig config) {
        return new WanSyncConfigHolder(config.getConsistencyCheckStrategy().getId());
    }

    @Nonnull
    WanBatchPublisherConfig toConfig(@Nonnull WanBatchPublisherConfigHolder holder) {
        checkNotNull(holder, "WAN batch publisher config holder must be provided");
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();
        config.setPublisherId(holder.getPublisherId());
        config.setProperties(toProperties(holder.getProperties()));

        // note. class name and implementation are mutually exclusive -- className seems the safer of the two
        boolean classNameProvided = holder.getClassName() != null;
        if (classNameProvided) {
            config.setClassName(holder.getClassName());
        }
        if (!classNameProvided && holder.getImplementation() != null) {
            WanPublisher wanPublisher = serializationService.toObject(holder.getImplementation());
            config.setImplementation(wanPublisher);
        }

        config.setClusterName(holder.getClusterName());
        config.setSnapshotEnabled(holder.isSnapshotEnabled());
        config.setInitialPublisherState(getWanPublisherState(holder.getInitialPublisherState()));
        config.setQueueCapacity(holder.getQueueCapacity());
        config.setBatchSize(holder.getBatchSize());
        config.setBatchMaxDelayMillis(holder.getBatchMaxDelayMillis());
        config.setResponseTimeoutMillis(holder.getResponseTimeoutMillis());
        config.setQueueFullBehavior(getWanQueueFullBehaviour(holder.getQueueFullBehavior()));
        config.setAcknowledgeType(getWanAcknowledgeType(holder.getAcknowledgeType()));
        config.setDiscoveryPeriodSeconds(holder.getDiscoveryPeriodSeconds());
        config.setMaxTargetEndpoints(holder.getMaxTargetEndpoints());
        config.setMaxConcurrentInvocations(holder.getMaxConcurrentInvocations());
        config.setUseEndpointPrivateAddress(holder.isUseEndpointPrivateAddress());
        config.setIdleMinParkNs(holder.getIdleMinParkNs());
        config.setIdleMaxParkNs(holder.getIdleMaxParkNs());
        config.setTargetEndpoints(holder.getTargetEndpoints());

        config.setAwsConfig(holder.getAwsConfig());
        config.setGcpConfig(holder.getGcpConfig());
        config.setAzureConfig(holder.getAzureConfig());
        config.setKubernetesConfig(holder.getKubernetesConfig());
        config.setEurekaConfig(holder.getEurekaConfig());
        config.setDiscoveryConfig(toConfig(holder.getDiscoveryConfig()));
        config.setSyncConfig(toConfig(holder.getSyncConfig()));

        config.setEndpoint(holder.getEndpoint());
        return config;
    }
}
