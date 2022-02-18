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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.internal.monitor.impl.LocalFlakeIdGeneratorStatsImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FLAKE_ID_GENERATOR_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

public class FlakeIdGeneratorService implements ManagedService, RemoteService,
                                                StatisticsAwareService<LocalFlakeIdGeneratorStats>, DynamicMetricsProvider {

    public static final String SERVICE_NAME = "hz:impl:flakeIdGeneratorService";

    private NodeEngine nodeEngine;
    private final ConcurrentHashMap<String, LocalFlakeIdGeneratorStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalFlakeIdGeneratorStatsImpl> localFlakeIdStatsConstructorFunction
        = key -> new LocalFlakeIdGeneratorStatsImpl();

    public FlakeIdGeneratorService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;

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
    public DistributedObject createDistributedObject(String name, UUID source, boolean local) {
        return new FlakeIdGeneratorProxy(name, nodeEngine, this, source);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        statsMap.remove(name);
    }

    @Override
    public Map<String, LocalFlakeIdGeneratorStats> getStats() {
        return new HashMap<>(statsMap);
    }

    /**
     * Updated the statistics for the {@link FlakeIdGenerator} with the given
     * name for a newly generated batch of the given size.
     *
     * @param name name of the generator, not null
     * @param batchSize size of the batch created
     */
    public void updateStatsForBatch(String name, int batchSize) {
        LocalFlakeIdGeneratorStatsImpl stats = getLocalFlakeIdStats(name);
        if (stats != null) {
            stats.update(batchSize);
        }
    }

    private LocalFlakeIdGeneratorStatsImpl getLocalFlakeIdStats(String name) {
        if (!nodeEngine.getConfig().getFlakeIdGeneratorConfig(name).isStatisticsEnabled()) {
            return null;
        }
        return getOrPutIfAbsent(statsMap, name, localFlakeIdStatsConstructorFunction);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, FLAKE_ID_GENERATOR_PREFIX, getStats());
    }
}
