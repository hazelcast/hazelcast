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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionlessSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Contains Raft-based semaphore instances
 */
public class SemaphoreService extends AbstractBlockingService<AcquireInvocationKey, Semaphore, SemaphoreRegistry>
        implements DynamicMetricsProvider {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public SemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void initImpl() {
        super.initImpl();
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    public boolean initSemaphore(CPGroupId groupId, String name, int permits) {
        try {
            Collection<AcquireInvocationKey> acquired = getOrInitRegistry(groupId).init(name, permits);
            notifyWaitKeys(groupId, name, acquired, true);

            return true;
        } catch (IllegalStateException ignored) {
            return false;
        }
    }

    private SemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(name);
    }

    public int availablePermits(CPGroupId groupId, String name) {
        SemaphoreRegistry registry = getOrInitRegistry(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public AcquireResult acquirePermits(CPGroupId groupId, String name, AcquireInvocationKey key, long timeoutMs) {
        heartbeatSession(groupId, key.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, key, timeoutMs);

        if (logger.isFineEnabled()) {
            if (result.status() == SUCCESSFUL) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else if (result.status() == WAIT_KEY_ADDED) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " wait key added for permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else if (result.status() == FAILED) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            }
        }

        notifyCancelledWaitKeys(groupId, name, result.cancelledWaitKeys());

        if (result.status() == WAIT_KEY_ADDED) {
            scheduleTimeout(groupId, name, key.invocationUid(), timeoutMs);
        }

        return result;
    }

    public void releasePermits(CPGroupId groupId, long commitIndex, String name, SemaphoreEndpoint endpoint, UUID invocationUid,
                               int permits) {
        heartbeatSession(groupId, endpoint.sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).release(name, endpoint, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelledWaitKeys());
        notifyWaitKeys(groupId, name, result.acquiredWaitKeys(), true);

        if (logger.isFineEnabled()) {
            if (result.success()) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " released permits: " + permits + " by <" + endpoint + ", "
                        + invocationUid + "> at commit index: " + commitIndex + " new acquires: " + result.acquiredWaitKeys());
            } else {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not-released permits: " + permits + " by <" + endpoint
                        + ", " + invocationUid + "> at commit index: " + commitIndex);
            }
        }

        if (!result.success()) {
            throw new IllegalStateException("Could not release " + permits + " permits in Semaphore[" + name
                    + "] because the caller has acquired less than " + permits + " permits");
        }
    }

    public int drainPermits(CPGroupId groupId, String name, long commitIndex, SemaphoreEndpoint endpoint, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).drainPermits(name, endpoint, invocationUid);
        notifyCancelledWaitKeys(groupId, name, result.cancelledWaitKeys());

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " drained permits: " + result.permits()
                    + " by <" + endpoint + ", " + invocationUid + "> at commit index: " + commitIndex);
        }

        return result.permits();
    }

    public boolean changePermits(CPGroupId groupId, long commitIndex, String name, SemaphoreEndpoint endpoint, UUID invocationUid,
                                 int permits) {
        heartbeatSession(groupId, endpoint.sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).changePermits(name, endpoint, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelledWaitKeys());
        notifyWaitKeys(groupId, name, result.acquiredWaitKeys(), true);

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " changed permits: " + permits + " by <" + endpoint + ", "
                    + invocationUid + "> at commit index: " + commitIndex + ". new acquires: " + result.acquiredWaitKeys());
        }

        return result.success();
    }

    private void notifyCancelledWaitKeys(CPGroupId groupId, String name, Collection<AcquireInvocationKey> keys) {
        if (keys.isEmpty()) {
            return;
        }

        notifyWaitKeys(groupId, name, keys, new WaitKeyCancelledException());
    }

    @Override
    protected SemaphoreRegistry createNewRegistry(CPGroupId groupId) {
        return new SemaphoreRegistry(groupId, raftService.getConfig());
    }

    @Override
    protected void onRegistryRestored(SemaphoreRegistry registry) {
        registry.setCpSubsystemConfig(raftService.getConfig());
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
    public ISemaphore createProxy(String proxyName) {
        try {
            proxyName = withoutDefaultGroupName(proxyName);
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            String objectName = getObjectNameForProxy(proxyName);
            SemaphoreConfig config = getConfig(proxyName);
            return config != null && config.isJDKCompatible()
                    ? new SessionlessSemaphoreProxy(nodeEngine, groupId, proxyName, objectName)
                    : new SessionAwareSemaphoreProxy(nodeEngine, groupId, proxyName, objectName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.semaphore");

        for (CPGroupId groupId : getGroupIdSet()) {
            SemaphoreRegistry registry = getRegistryOrNull(groupId);
            for (Semaphore sema : registry.getAllSemaphores()) {
                MetricDescriptor desc = root.copy()
                        .withDiscriminator("id", sema.getName() + "@" + groupId.getName())
                        .withTag(CP_TAG_NAME, sema.getName())
                        .withTag("group", groupId.getName());

                context.collect(desc.copy().withMetric("initialized"), sema.isInitialized() ? 1 : 0);
                context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("available"), sema.getAvailable());
            }
        }
    }
}
