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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.proxy.FencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains Raft-based lock instances
 */
public class LockService extends AbstractBlockingService<LockInvocationKey, Lock, LockRegistry>
        implements DynamicMetricsProvider {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:lockService";

    private final ConcurrentMap<String, FencedLockProxy> proxies = new ConcurrentHashMap<>();

    public LockService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected void initImpl() {
        super.initImpl();
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    public AcquireResult acquire(CPGroupId groupId, String name, LockInvocationKey key, long timeoutMs) {
        heartbeatSession(groupId, key.sessionId());
        LockRegistry registry = getOrInitRegistry(groupId);
        AcquireResult result = registry.acquire(name, key, timeoutMs);

        if (logger.isFineEnabled()) {
            if (result.status() == SUCCESSFUL) {
                logger.fine("Lock[" + name + "] in " + groupId + " acquired by <" + key.endpoint() + ", " + key.invocationUid()
                        + "> at commit index: " + key.commitIndex() + ". new lock state: "
                        + registry.getLockOwnershipState(name));
            } else if (result.status() == WAIT_KEY_ADDED) {
                logger.fine("Lock[" + name + "] in " + groupId + " wait key added for <" + key.endpoint() + ", "
                        + key.invocationUid() + "> at commit index: " + key.commitIndex() + ". lock state: "
                        + registry.getLockOwnershipState(name));
            } else if (result.status() == FAILED) {
                logger.fine("Lock[" + name + "] in " + groupId + " acquire failed for <" + key.endpoint() + ", "
                        + key.invocationUid() + "> at commit index: " + key.commitIndex() + ". lock state: "
                        + registry.getLockOwnershipState(name));
            }
        }

        if (result.status() == WAIT_KEY_ADDED) {
            scheduleTimeout(groupId, name, key.invocationUid(), timeoutMs);
        }

        notifyCancelledWaitKeys(groupId, name, result.cancelledWaitKeys());

        return result;
    }

    public boolean release(CPGroupId groupId, long commitIndex, String name, LockEndpoint endpoint, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        LockRegistry registry = getLockRegistryOrFail(groupId, name);
        ReleaseResult result = registry.release(name, endpoint, invocationUid);

        if (logger.isFineEnabled()) {
            if (result.success()) {
                logger.fine("Lock[" + name + "] in " + groupId + " released by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". new lock state: " + result.ownership());
            } else {
                logger.fine("Lock[" + name + "] in " + groupId + " not released by <" + endpoint + ", " + invocationUid
                        + "> at commit index: " + commitIndex + ". lock state: " + registry.getLockOwnershipState(name));
            }
        }

        if (result.success()) {
            notifyWaitKeys(groupId, name, result.completedWaitKeys(), result.ownership().getFence());
            return result.ownership().isLockedBy(endpoint.sessionId(), endpoint.threadId());
        }

        notifyCancelledWaitKeys(groupId, name, result.completedWaitKeys());
        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    private void notifyCancelledWaitKeys(CPGroupId groupId, String name, Collection<LockInvocationKey> keys) {
        if (keys.isEmpty()) {
            return;
        }

        notifyWaitKeys(groupId, name, keys, new WaitKeyCancelledException());
    }

    public LockOwnershipState getLockOwnershipState(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.getLockOwnershipState(name) : LockOwnershipState.NOT_LOCKED;
    }

    private LockRegistry getLockRegistryOrFail(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        LockRegistry registry = getRegistryOrNull(groupId);
        if (registry == null) {
            throw new IllegalMonitorStateException("Lock registry of " + groupId + " not found for Lock[" + name + "]");
        }

        return registry;
    }

    @Override
    protected LockRegistry createNewRegistry(CPGroupId groupId) {
        return new LockRegistry(nodeEngine.getConfig().getCPSubsystemConfig(), groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return FencedLock.INVALID_FENCE;
    }

    @Override
    protected void onRegistryRestored(LockRegistry registry) {
        registry.setCpSubsystemConfig(nodeEngine.getConfig().getCPSubsystemConfig());
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public FencedLock createProxy(String proxyName) {
        proxyName = withoutDefaultGroupName(proxyName);

        while (true) {
            FencedLockProxy proxy = proxies.get(proxyName);
            if (proxy != null) {
                CPGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
                if (!proxy.getGroupId().equals(groupId)) {
                    proxies.remove(proxyName, proxy);
                } else {
                    return proxy;
                }
            }

            proxy = doCreateProxy(proxyName);
            FencedLockProxy existing = proxies.putIfAbsent(proxyName, proxy);
            if (existing == null) {
                return proxy;
            }
        }
    }

    @Override
    public void onCPSubsystemRestart() {
        super.onCPSubsystemRestart();
        proxies.clear();
    }

    private FencedLockProxy doCreateProxy(String proxyName) {
        try {
            RaftGroupId groupId = raftService.createRaftGroupForProxy(proxyName);
            return new FencedLockProxy(nodeEngine, groupId, proxyName, getObjectNameForProxy(proxyName));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.lock");

        for (CPGroupId groupId : getGroupIdSet()) {
            LockRegistry registry = getRegistryOrNull(groupId);
            for (Lock lock : registry.getAllLocks()) {
                MetricDescriptor desc = root.copy()
                        .withDiscriminator("id", lock.getName() + "@" + groupId.getName())
                        .withTag(CP_TAG_NAME, lock.getName())
                        .withTag("group", groupId.getName());

                context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("acquireLimit"), lock.lockCountLimit());

                LockInvocationKey owner = lock.owner();
                int lockCount = lock.lockCount();

                // We are reading two separate volatile fields but not atomically.
                // We may observe a partial update.
                if (owner != null && lockCount > 0) {
                    context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("lockCount"), lockCount);
                    context.collect(desc.copy().withMetric("ownerSessionId"), owner.sessionId());
                    context.collect(desc.copy().withTag("owner", owner.callerAddress().toString()).withMetric("owner"), 0);
                } else {
                    context.collect(desc.copy().withUnit(ProbeUnit.COUNT).withMetric("lockCount"), 0);
                }
            }
        }
    }

}
