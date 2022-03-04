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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map;
import java.util.UUID;

/**
 * Dynamic configurations.
 * <p>
 * Registers dynamic configurations in a local member and also broadcasts configuration to all cluster members.
 * <p>
 * <b>Note:</b> Implementations should do pattern matching on their own.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ConfigurationService {

    String SERVICE_NAME = "hz:configurationService";

    /**
     * Registers a dynamic configurations to all cluster members.
     *
     * @param config configuration to register
     * @throws InvalidConfigurationException when static configuration already
     *                                       contains the same config with the
     *                                       same name
     */
    void broadcastConfig(IdentifiedDataSerializable config);

    /**
     * Update the license for the cluster.
     *
     * @param licenseKey new license key to set
     */
    void updateLicense(String licenseKey);

    /**
     * Persists any dynamically changeable sub configuration to this member's
     * declarative configuration. Preserves file format of the existing dynamic
     * configuration persistence file. Also note that this method is
     * idempotent.
     *
     * @param subConfig configuration to persist
     */
    void persist(Object subConfig);

    /**
     * Updates the configuration with the given configuration. Updating means
     * dynamically changing all the differences dynamically changeable.
     *
     * @param newConfig config to find any new dynamically changeable sub configs
     * @return update result which includes added and ignored configurations
     */
    ConfigUpdateResult update(Config newConfig);

    /**
     * Updates the configuration with the declarative configuration. Updating
     * means dynamically changing all the differences dynamically changeable.
     *
     * @return update result which includes added and ignored configurations
     */
    default ConfigUpdateResult update() {
        return update(null);
    }

    /**
     * Starts a configuration update process asynchronously. Updates the configuration with the given configuration. Updating
     * means dynamically changing all the differences dynamically changeable.
     *
     * @param configPatch string representation of the config patch, to find any new dynamically changeable sub configs
     * @return the unique identifier of the config update process. The MC Events emitted during the process will have the same
     * {@link com.hazelcast.internal.management.events.AbstractConfigUpdateEvent#getConfigUpdateProcessId()}.
     */
    UUID updateAsync(String configPatch);

    default UUID updateAsync() {
        return updateAsync(null);
    }

    /**
     * Finds existing Multimap config.
     *
     * @param name name of the config
     * @return Multimap config or {@code null} when requested MultiMap configuration does not exist
     */
    MultiMapConfig findMultiMapConfig(String name);

    /**
     * Finds existing Map config.
     *
     * @param name name of the config
     * @return Map config or {@code null} when requested Map configuration does not exist
     */
    MapConfig findMapConfig(String name);

    /**
     * Finds existing Topic config.
     *
     * @param name name of the config
     * @return Topic config or {@code null} when requested Topic configuration does not exist
     */
    TopicConfig findTopicConfig(String name);

    /**
     * Finds existing Cardinality Estimator config.
     *
     * @param name name of the config
     * @return Cardinality Estimator config or {@code null} when requested Cardinality Estimator configuration does not exist
     */
    CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name);

    /**
     * Finds existing PN counter config.
     *
     * @param name name of the config
     * @return PN counter config or {@code null} when requested PN counter configuration does not exist
     */
    PNCounterConfig findPNCounterConfig(String name);

    /**
     * Finds existing Executor config.
     *
     * @param name name of the config
     * @return Executor config or {@code null} when requested Executor configuration does not exist
     */
    ExecutorConfig findExecutorConfig(String name);

    /**
     * Finds existing Scheduled Executor config.
     *
     * @param name name of the config
     * @return Scheduled Executor config or {@code null} when requested Scheduled Executor configuration does not exist
     */
    ScheduledExecutorConfig findScheduledExecutorConfig(String name);

    /**
     * Finds existing Durable Executor config.
     *
     * @param name name of the config
     * @return Durable Executor config or {@code null} when requested Durable Executor configuration does not exist
     */
    DurableExecutorConfig findDurableExecutorConfig(String name);

    /**
     * Finds existing Ringbuffer config.
     *
     * @param name name of the config
     * @return Ringbuffer config or {@code null} when requested Ringbuffer configuration does not exist
     */
    RingbufferConfig findRingbufferConfig(String name);

    /**
     * Finds existing List config.
     *
     * @param name name of the config
     * @return List config or {@code null} when requested List configuration does not exist
     */
    ListConfig findListConfig(String name);

    /**
     * Finds existing Queue config.
     *
     * @param name name of the config
     * @return Queue config or {@code null} when requested Queue configuration does not exist
     */
    QueueConfig findQueueConfig(String name);

    /**
     * Finds existing Set config.
     *
     * @param name name of the config
     * @return Set config or {@code null} when requested Set configuration does not exist
     */
    SetConfig findSetConfig(String name);

    /**
     * Finds existing ReplicatedMap config.
     *
     * @param name name of the config
     * @return ReplicatedMap config or {@code null} when requested ReplicatedMap configuration does not exist
     */
    ReplicatedMapConfig findReplicatedMapConfig(String name);

    /**
     * Finds existing Reliable Topic config.
     *
     * @param name name of the config
     * @return Reliable Topic config or {@code null} when requested Reliable Topic configuration does not exist
     */
    ReliableTopicConfig findReliableTopicConfig(String name);

    /**
     * Finds existing Cache config.
     *
     * @param name name of the config
     * @return Cache config or {@code null} when requested Cache configuration does not exist
     */
    CacheSimpleConfig findCacheSimpleConfig(String name);

    /**
     * Finds existing FlakeIdGeneratorConfig config.
     *
     * @param name name of the config
     * @return FlakeIdGenerator config or {@code null} when requested FlakeIdGenerator configuration does not exist
     */
    FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name);

    /**
     * Returns all registered map configurations.
     *
     * @return registered map configurations
     */
    Map<String, MapConfig> getMapConfigs();

    /**
     * Returns all registered queue configurations.
     *
     * @return registered queue configurations
     */
    Map<String, QueueConfig> getQueueConfigs();

    /**
     * Returns all registered list configurations.
     *
     * @return registered list configurations
     */
    Map<String, ListConfig> getListConfigs();

    /**
     * Returns all registered set configurations.
     *
     * @return registered set configurations
     */
    Map<String, SetConfig> getSetConfigs();

    /**
     * Returns all registered multimap configurations.
     *
     * @return registered multimap configurations
     */
    Map<String, MultiMapConfig> getMultiMapConfigs();

    /**
     * Returns all registered replicated map configurations.
     *
     * @return registered replicated map configurations
     */
    Map<String, ReplicatedMapConfig> getReplicatedMapConfigs();

    /**
     * Returns all registered ringbuffer configurations.
     *
     * @return registered ringbuffer configurations
     */
    Map<String, RingbufferConfig> getRingbufferConfigs();

    /**
     * Returns all registered topic configurations.
     *
     * @return registered topic configurations
     */
    Map<String, TopicConfig> getTopicConfigs();

    /**
     * Returns all registered reliable topic configurations.
     *
     * @return registered reliable topic configurations
     */
    Map<String, ReliableTopicConfig> getReliableTopicConfigs();

    /**
     * Returns all registered executor configurations.
     *
     * @return registered executor configurations
     */
    Map<String, ExecutorConfig> getExecutorConfigs();

    /**
     * Returns all registered durable executor configurations.
     *
     * @return registered durable executor configurations
     */
    Map<String, DurableExecutorConfig> getDurableExecutorConfigs();

    /**
     * Returns all registered scheduled executor configurations.
     *
     * @return registered scheduled executor configurations
     */
    Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs();

    /**
     * Returns all registered cardinality estimator configurations.
     *
     * @return registered cardinality estimator configurations
     */
    Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs();

    /**
     * Returns all registered PN counter configurations.
     *
     * @return registered PN counter configurations
     */
    Map<String, PNCounterConfig> getPNCounterConfigs();

    /**
     * Returns all registered cache configurations.
     *
     * @return registered cache configurations
     */
    Map<String, CacheSimpleConfig> getCacheSimpleConfigs();

    /**
     * Returns all registered FlakeIdGenerator configurations.
     *
     * @return registered FlakeIdGenerator configurations
     */
    Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs();
}
