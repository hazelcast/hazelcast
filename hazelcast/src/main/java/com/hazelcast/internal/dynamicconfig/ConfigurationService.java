/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map;

/**
 * Dynamic configurations.
 * Register dynamic configurations in a local member and also can broadcast configuration to all cluster members.
 *
 * Implementations should do pattern matching on its own.
 *
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ConfigurationService {

    /**
     * Register a dynamic configurations to all cluster members.
     *
     * @param config Configuration to register
     * @throws com.hazelcast.config.ConfigurationException when static configuration already contains the same config
     * with the same name
     */
    void broadcastConfig(IdentifiedDataSerializable config);

    /**
     * Find existing Multimap Config
     *
     * @param name
     * @return Multimap Config or null when requested MultiMap configuration does not exist
     */
    MultiMapConfig findMultiMapConfig(String name);

    /**
     * Find existing Map Config
     *
     * @param name
     * @return Map Config or null when requested Map configuration does not exist
     */
    MapConfig findMapConfig(String name);


    /**
     * Find existing Topic Config
     *
     * @param name
     * @return Topic Config or null when requested Topic configuration does not exist
     */
    TopicConfig findTopicConfig(String name);

    /**
     * Find existing Cardinality Estimator Config
     *
     * @param name
     * @return Cardinality Estimator Config or null when requested Cardinality Estimator configuration does not exist
     */
    CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name);

    /**
     * Find existing Executor Config
     *
     * @param name
     * @return Executor Config or null when requested Executor configuration does not exist
     */
    ExecutorConfig findExecutorConfig(String name);

    /**
     * Find existing Scheduled Executor Config
     *
     * @param name
     * @return Scheduled Executor Config or null when requested Scheduled Executor configuration does not exist
     */
    ScheduledExecutorConfig findScheduledExecutorConfig(String name);

    /**
     * Find existing Durable Executor Config
     *
     * @param name
     * @return Durable Executor Config or null when requested Durable Executor configuration does not exist
     */
    DurableExecutorConfig findDurableExecutorConfig(String name);

    /**
     * Find existing Semaphore Config
     *
     * @param name
     * @return Semaphore Config or null when requested Semaphore configuration does not exist
     */
    SemaphoreConfig findSemaphoreConfig(String name);

    /**
     * Find existing Ringbuffer Config
     *
     * @param name
     * @return Ringbuffer Config or null when requested Ringbuffer configuration does not exist
     */
    RingbufferConfig findRingbufferConfig(String name);

    /**
     * Find existing Lock Config
     *
     * @param name
     * @return Lock Config or null when requested Lock configuration does not exist
     */
    LockConfig findLockConfig(String name);

    /**
     * Find existing List Config
     *
     * @param name
     * @return MaListp Config or null when requested List configuration does not exist
     */
    ListConfig findListConfig(String name);

    /**
     * Find existing Queue Config
     *
     * @param name
     * @return Queue Config or null when requested Queue configuration does not exist
     */
    QueueConfig findQueueConfig(String name);

    /**
     * Find existing Set Config
     *
     * @param name
     * @return Set Config or null when requested Set configuration does not exist
     */
    SetConfig findSetConfig(String name);

    /**
     * Find existing Replicated Map Config
     *
     * @param name
     * @return Replicated Map Config or null when requested Replicated Map configuration does not exist
     */
    ReplicatedMapConfig findReplicatedMapConfig(String name);

    /**
     * Find existing Reliable Topic Config
     *
     * @param name
     * @return Reliable Topic Config or null when requested Reliable Topic configuration does not exist
     */
    ReliableTopicConfig findReliableTopicConfig(String name);

    /**
     * Find existing Cache Config
     *
     * @param name
     * @return Cache Config or null when requested Cache configuration does not exist
     */
    CacheSimpleConfig findCacheSimpleConfig(String name);

    /**
     * Return all registered map configurations.
     *
     * @return registered map configurations
     */
    Map<String, MapConfig> getMapConfigs();

    /**
     * Return all registered lock configurations.
     *
     * @return registered lock configurations
     */
    Map<String, LockConfig> getLockConfigs();

    /**
     * Return all registered queue configurations.
     *
     * @return registered queue configurations
     */
    Map<String, QueueConfig> getQueueConfigs();

    /**
     * Return all registered list configurations.
     *
     * @return registered list configurations
     */
    Map<String, ListConfig> getListConfigs();

    /**
     * Return all registered set configurations.
     *
     * @return registered set configurations
     */
    Map<String, SetConfig> getSetConfigs();

    /**
     * Return all registered multimap configurations.
     *
     * @return registered multimap configurations
     */
    Map<String, MultiMapConfig> getMultiMapConfigs();

    /**
     * Return all registered replicated map configurations.
     *
     * @return registered replicated map configurations
     */
    Map<String, ReplicatedMapConfig> getReplicatedMapConfigs();

    /**
     * Return all registered ringbuffer configurations.
     *
     * @return registered ringbuffer configurations
     */
    Map<String, RingbufferConfig> getRingbufferConfigs();

    /**
     * Return all registered topic configurations.
     *
     * @return registered topic configurations
     */
    Map<String, TopicConfig> getTopicConfigs();

    /**
     * Return all registered reliable topic configurations.
     *
     * @return registered reliable topic configurations
     */
    Map<String, ReliableTopicConfig> getReliableTopicConfigs();

    /**
     * Return all registered executor configurations.
     *
     * @return registered executor configurations
     */
    Map<String, ExecutorConfig> getExecutorConfigs();

    /**
     * Return all registered durable executor configurations.
     *
     * @return registered durable executor configurations
     */
    Map<String, DurableExecutorConfig> getDurableExecutorConfigs();

    /**
     * Return all registered scheduled executor configurations.
     *
     * @return registered scheduled executor configurations
     */
    Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs();

    /**
     * Return all registered cardinality estimator configurations.
     *
     * @return registered cardinality estimator configurations
     */
    Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs();

    /**
     * Return all registered semaphore configurations.
     *
     * @return registered semaphore configurations
     */
    Map<String, SemaphoreConfig> getSemaphoreConfigs();

    /**
     * Return all registered cache configurations.
     *
     * @return registered cache configurations
     */
    Map<String, CacheSimpleConfig> getCacheSimpleConfigs();

    EventJournalConfig findCacheEventJournalConfig(String baseName);

    Map<String, EventJournalConfig> getCacheEventJournalConfigs();

    EventJournalConfig findMapEventJournalConfig(String baseName);

    Map<String, EventJournalConfig> getMapEventJournalConfigs();
}
