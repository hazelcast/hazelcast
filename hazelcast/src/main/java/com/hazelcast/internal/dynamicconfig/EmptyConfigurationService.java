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
import com.hazelcast.config.ReliableIdGeneratorConfig;
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

import static java.util.Collections.emptyMap;

/**
 * This is used when Hazelcast is starting and {@link ClusterWideConfigurationService} is not available yet.
 */
@SuppressWarnings("checkstyle:methodcount")
class EmptyConfigurationService implements ConfigurationService {

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return null;
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return null;
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return null;
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return null;
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return null;
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return null;
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return null;
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return null;
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return null;
    }

    @Override
    public AtomicLongConfig findAtomicLongConfig(String name) {
        return null;
    }

    @Override
    public AtomicReferenceConfig findAtomicReferenceConfig(String name) {
        return null;
    }

    @Override
    public CountDownLatchConfig findCountDownLatchConfig(String name) {
        return null;
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return null;
    }

    @Override
    public ListConfig findListConfig(String name) {
        return null;
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return null;
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return null;
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return null;
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return null;
    }

    @Override
    public CacheSimpleConfig findCacheSimpleConfig(String name) {
        return null;
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheSimpleConfigs() {
        return emptyMap();
    }

    @Override
    public EventJournalConfig findCacheEventJournalConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        return emptyMap();
    }

    @Override
    public EventJournalConfig findMapEventJournalConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, AtomicLongConfig> getAtomicLongConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, AtomicReferenceConfig> getAtomicReferenceConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, CountDownLatchConfig> getCountDownLatchConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, SemaphoreConfig> getSemaphoreConfigs() {
        return emptyMap();
    }

    @Override
    public ReliableIdGeneratorConfig findReliableIdGeneratorConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, ReliableIdGeneratorConfig> getReliableIdGeneratorConfigs() {
        return emptyMap();
    }

    @Override
    public void broadcastConfig(IdentifiedDataSerializable config) {
        throw new IllegalStateException("Cannot add a new config while Hazelcast is starting.");
    }

}
