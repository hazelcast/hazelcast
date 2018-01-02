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

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableIdGeneratorConfig;
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
 * <p>
 * Registers dynamic configurations in a local member and also broadcasts configuration to all cluster members.
 * <p>
 * <b>Note:</b> Implementations should do pattern matching on their own.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ConfigurationService {

    /**
     * Registers a dynamic configurations to all cluster members.
     *
     * @param config configuration to register
     * @throws com.hazelcast.config.ConfigurationException when static configuration already contains the same config
     *                                                     with the same name
     */
    void broadcastConfig(IdentifiedDataSerializable config);

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
     * Finds existing Semaphore config.
     *
     * @param name name of the config
     * @return Semaphore config or {@code null} when requested Semaphore configuration does not exist
     */
    SemaphoreConfig findSemaphoreConfig(String name);

    /**
     * Finds existing Ringbuffer config.
     *
     * @param name name of the config
     * @return Ringbuffer config or {@code null} when requested Ringbuffer configuration does not exist
     */
    RingbufferConfig findRingbufferConfig(String name);

    /**
     * Finds existing AtomicLong config.
     *
     * @return AtomicLong Config or {@code null} when requested AtomicLong configuration does not exist
     */
    AtomicLongConfig findAtomicLongConfig(String name);

    /**
     * Finds existing AtomicReference config.
     *
     * @return AtomicReference Config or {@code null} when requested AtomicReference configuration does not exist
     */
    AtomicReferenceConfig findAtomicReferenceConfig(String name);

    /**
     * Finds existing CountDownLatch config.
     *
     * @return CountDownLatch Config or {@code null} when requested CountDownLatch configuration does not exist
     */
    CountDownLatchConfig findCountDownLatchConfig(String name);

    /**
     * Finds existing Lock config.
     *
     * @param name name of the config
     * @return Lock config or {@code null} when requested Lock configuration does not exist
     */
    LockConfig findLockConfig(String name);

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
     * Finds existing CacheEventJournal config.
     *
     * @param name name of the config
     * @return CacheEventJournal config or {@code null} when requested CacheEventJournal configuration does not exist
     */
    EventJournalConfig findCacheEventJournalConfig(String name);

    /**
     * Finds existing MapEventJournal config.
     *
     * @param name name of the config
     * @return MapEventJournal config or {@code null} when requested MapEventJournal configuration does not exist
     */
    EventJournalConfig findMapEventJournalConfig(String name);

    /**
     * Finds existing ReliableIdGeneratorConfig config.
     *
     * @param name name of the config
     * @return ReliableIdGenerator config or {@code null} when requested ReliableIdGenerator configuration does not exist
     */
    ReliableIdGeneratorConfig findReliableIdGeneratorConfig(String name);

    /**
     * Returns all registered map configurations.
     *
     * @return registered map configurations
     */
    Map<String, MapConfig> getMapConfigs();

    /**
     * Returns all registered lock configurations.
     *
     * @return registered lock configurations
     */
    Map<String, LockConfig> getLockConfigs();

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
     * Returns all registered AtomicLong configurations.
     *
     * @return registered AtomicLong configurations
     */
    Map<String, AtomicLongConfig> getAtomicLongConfigs();

    /**
     * Returns all registered AtomicReference configurations.
     *
     * @return registered AtomicReference configurations
     */
    Map<String, AtomicReferenceConfig> getAtomicReferenceConfigs();

    /**
     * Returns all registered CountDownLatchConfig configurations.
     *
     * @return registered CountDownLatchConfig configurations
     */
    Map<String, CountDownLatchConfig> getCountDownLatchConfigs();

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
     * Returns all registered semaphore configurations.
     *
     * @return registered semaphore configurations
     */
    Map<String, SemaphoreConfig> getSemaphoreConfigs();

    /**
     * Returns all registered cache configurations.
     *
     * @return registered cache configurations
     */
    Map<String, CacheSimpleConfig> getCacheSimpleConfigs();

    /**
     * Returns all registered CacheEventJournal configurations.
     *
     * @return registered CacheEventJournal configurations
     */
    Map<String, EventJournalConfig> getCacheEventJournalConfigs();

    /**
     * Returns all registered MapEventJournal configurations.
     *
     * @return registered MapEventJournal configurations
     */
    Map<String, EventJournalConfig> getMapEventJournalConfigs();

    /**
     * Returns all registered ReliableIdGenerator configurations.
     *
     * @return registered ReliableIdGenerator configurations
     */
    Map<String, ReliableIdGeneratorConfig> getReliableIdGeneratorConfigs();
}
