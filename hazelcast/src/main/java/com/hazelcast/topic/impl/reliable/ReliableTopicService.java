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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.topic.LocalTopicStats;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.RELIABLE_TOPIC_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class ReliableTopicService implements ManagedService, RemoteService,
        StatisticsAwareService<LocalTopicStats>, DynamicMetricsProvider {

    public static final String SERVICE_NAME = "hz:impl:reliableTopicService";
    private final ConcurrentMap<String, LocalTopicStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalTopicStatsImpl> localTopicStatsConstructorFunction =
        mapName -> new LocalTopicStatsImpl();

    private final NodeEngine nodeEngine;

    public ReliableTopicService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        ReliableTopicConfig topicConfig = nodeEngine.getConfig().findReliableTopicConfig(objectName);
        return new ReliableTopicProxy(objectName, nodeEngine, this, topicConfig);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        statsMap.remove(objectName);
    }

    /**
     * Returns reliable topic statistics local to this member
     * for the reliable topic with {@code name}.
     *
     * @param name the name of the reliable topic
     * @return the statistics local to this member
     */
    public LocalTopicStatsImpl getLocalTopicStats(String name) {
        return getOrPutSynchronized(statsMap, name, statsMap, localTopicStatsConstructorFunction);
    }

    @Override
    public Map<String, LocalTopicStats> getStats() {
        Map<String, LocalTopicStats> topicStats = MapUtil.createHashMap(statsMap.size());
        Config config = nodeEngine.getConfig();
        for (Map.Entry<String, LocalTopicStatsImpl> queueStat : statsMap.entrySet()) {
            String name = queueStat.getKey();
            if (config.getReliableTopicConfig(name).isStatisticsEnabled()) {
                topicStats.put(name, queueStat.getValue());
            }
        }
        return topicStats;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    @Override
    public void reset() {
        statsMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, RELIABLE_TOPIC_PREFIX, getStats());
    }
}
