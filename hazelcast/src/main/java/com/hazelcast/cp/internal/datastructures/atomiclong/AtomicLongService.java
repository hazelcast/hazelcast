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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;

/**
 * Contains Raft-based atomic long instances, implements snapshotting,
 * and creates proxies
 */
public class AtomicLongService extends RaftAtomicValueService<Long, AtomicLong, AtomicLongSnapshot>
        implements DynamicMetricsProvider {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:atomicLongService";

    public AtomicLongService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        super.init(nodeEngine, properties);
        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.registerDynamicMetricsProvider(this);
    }

    @Override
    protected AtomicLongSnapshot newSnapshot(Map<String, Long> values, Set<String> destroyed) {
        return new AtomicLongSnapshot(values, destroyed);
    }

    @Override
    protected AtomicLong newAtomicValue(CPGroupId groupId, String name, Long val) {
        return new AtomicLong(groupId, name, val != null ? val : 0L);
    }

    @Override
    protected IAtomicLong newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId, String proxyName,
            String objectNameForProxy) {
        return new AtomicLongProxy(nodeEngine, groupId, proxyName, objectNameForProxy);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.atomiclong");
        for (AtomicLong value : atomicValues.values()) {
            CPGroupId groupId = value.groupId();
            MetricDescriptor desc = root.copy()
                    .withDiscriminator("id", value.name() + "@" + groupId.getName())
                    .withTag(CP_TAG_NAME, value.name())
                    .withTag("group", groupId.getName())
                    .withMetric("value");
            context.collect(desc, value.value());
        }
    }
}
