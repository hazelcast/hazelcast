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
    public PNCounterConfig findPNCounterConfig(String name) {
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
    public RingbufferConfig findRingbufferConfig(String name) {
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
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        return emptyMap();
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return emptyMap();
    }

    @Override
    public void broadcastConfig(IdentifiedDataSerializable config) {
        throw new IllegalStateException("Cannot add a new config while Hazelcast is starting.");
    }

    @Override
    public void persist(Object subConfig) {
        // Code shouldn't come here. broadcastConfig() will throw an exception
        // before here.
        throw new IllegalStateException("Cannot add a new config while Hazelcast is starting.");
    }

    @Override
    public ConfigUpdateResult update(Config newConfig) {
        throw new IllegalStateException("Cannot reload config while Hazelcast is starting.");
    }

    @Override
    public UUID updateAsync(String configPatch) {
        throw new IllegalStateException("Cannot reload config while Hazelcast is starting.");
    }

    @Override
    public void updateLicense(String licenseKey) {
        throw new IllegalStateException("Cannot update license while Hazelcast is starting.");
    }
}
