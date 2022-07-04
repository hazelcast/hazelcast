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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.proxy.CountDownLatchProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Contains Raft-based count down latch instances
 */
public class CountDownLatchService extends AbstractBlockingService<AwaitInvocationKey, CountDownLatch, CountDownLatchRegistry>
        implements DynamicMetricsProvider {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:countDownLatchService";

    public CountDownLatchService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void initImpl() {
        super.initImpl();
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    public boolean trySetCount(CPGroupId groupId, String name, int count) {
        return getOrInitRegistry(groupId).trySetCount(name, count);
    }

    public int countDown(CPGroupId groupId, String name, UUID invocationUuid, int expectedRound) {
        CountDownLatchRegistry registry = getOrInitRegistry(groupId);
        BiTuple<Integer, Collection<AwaitInvocationKey>> t = registry.countDown(name, invocationUuid, expectedRound);
        notifyWaitKeys(groupId, name, t.element2, true);

        return t.element1;
    }

    public boolean await(CPGroupId groupId, String name, AwaitInvocationKey key, long timeoutMillis) {
        boolean success = getOrInitRegistry(groupId).await(name, key, timeoutMillis);
        if (!success) {
            scheduleTimeout(groupId, name, key.invocationUid(), timeoutMillis);
        }

        return success;
    }

    public int getRemainingCount(CPGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRemainingCount(name);
    }

    public int getRound(CPGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRound(name);
    }

    @Override
    protected CountDownLatchRegistry createNewRegistry(CPGroupId groupId) {
        return new CountDownLatchRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public ICountDownLatch createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
            RaftGroupId groupId = service.createRaftGroupForProxy(proxyName);
            return new CountDownLatchProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    protected void onRegistryRestored(CountDownLatchRegistry registry) {
        super.onRegistryRestored(registry);
        for (CountDownLatch latch : registry.getAllLatches()) {
            latch.updateRemainingCount();
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.countdownlatch");

        for (CPGroupId groupId : getGroupIdSet()) {
            CountDownLatchRegistry registry = getRegistryOrNull(groupId);
            for (CountDownLatch latch : registry.getAllLatches()) {
                MetricDescriptor desc = root.copy()
                        .withDiscriminator("id", latch.getName() + "@" + groupId.getName())
                        .withTag(CP_TAG_NAME, latch.getName())
                        .withTag("group", groupId.getName());

                context.collect(desc.copy().withMetric("round"), latch.getRound());
                context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("count"), latch.getCount());
                context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("remaining"), latch.getRemainingCount());
            }
        }
    }
}
