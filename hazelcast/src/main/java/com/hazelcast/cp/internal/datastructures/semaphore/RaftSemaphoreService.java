/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.AcquireResult;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Contains Raft-based semaphore instances
 */
public class RaftSemaphoreService extends AbstractBlockingService<AcquireInvocationKey, RaftSemaphore, RaftSemaphoreRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public RaftSemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    private CPSemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(name);
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

    public int availablePermits(CPGroupId groupId, String name) {
        RaftSemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public boolean acquirePermits(CPGroupId groupId, String name, AcquireInvocationKey key, long timeoutMs) {
        heartbeatSession(groupId, key.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, key, timeoutMs);

        boolean success = (result.acquired > 0);
        if (logger.isFineEnabled()) {
            if (success) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else if (timeoutMs != 0) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " wait key added for permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            } else {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not acquired permits: " + key.permits() + " by <"
                        + key.endpoint() + ", " + key.invocationUid() + "> at commit index: " + key.commitIndex());
            }
        }

        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        if (!success) {
            scheduleTimeout(groupId, name, key.invocationUid(), timeoutMs);
        }

        return success;
    }

    public void releasePermits(CPGroupId groupId, long commitIndex, String name, SemaphoreEndpoint endpoint, UUID invocationUid,
                               int permits) {
        heartbeatSession(groupId, endpoint.sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).release(name, endpoint, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);
        notifyWaitKeys(groupId, name, result.acquired, true);

        if (logger.isFineEnabled()) {
            if (result.success) {
                logger.fine("Semaphore[" + name + "] in " + groupId + " released permits: " + permits + " by <" + endpoint + ", "
                        + invocationUid + "> at commit index: " + commitIndex + " new acquires: " + result.acquired);
            } else {
                logger.fine("Semaphore[" + name + "] in " + groupId + " not-released permits: " + permits + " by <" + endpoint
                        + ", " + invocationUid + "> at commit index: " + commitIndex);
            }
        }

        if (!result.success) {
            throw new IllegalArgumentException();
        }
    }

    public int drainPermits(CPGroupId groupId, String name, long commitIndex, SemaphoreEndpoint endpoint, UUID invocationUid) {
        heartbeatSession(groupId, endpoint.sessionId());
        AcquireResult result = getOrInitRegistry(groupId).drainPermits(name, endpoint, invocationUid);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " drained permits: " + result.acquired
                    + " by <" + endpoint + ", " + invocationUid + "> at commit index: " + commitIndex);
        }

        return result.acquired;
    }

    public boolean changePermits(CPGroupId groupId, long commitIndex, String name, SemaphoreEndpoint endpoint, UUID invocationUid,
                                 int permits) {
        heartbeatSession(groupId, endpoint.sessionId());
        ReleaseResult result = getOrInitRegistry(groupId).changePermits(name, endpoint, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);
        notifyWaitKeys(groupId, name, result.acquired, true);

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " changed permits: " + permits + " by <" + endpoint + ", "
                    + invocationUid + "> at commit index: " + commitIndex + ". new acquires: " + result.acquired);
        }

        return result.success;
    }

    private void notifyCancelledWaitKeys(CPGroupId groupId, String name, Collection<AcquireInvocationKey> keys) {
        if (keys.isEmpty()) {
            return;
        }

        notifyWaitKeys(groupId, name, keys, new WaitKeyCancelledException());
    }

    @Override
    protected RaftSemaphoreRegistry createNewRegistry(CPGroupId groupId) {
        return new RaftSemaphoreRegistry(groupId);
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
            CPSemaphoreConfig config = getConfig(proxyName);
            return config != null && config.isJDKCompatible()
                    ? new RaftSessionlessSemaphoreProxy(nodeEngine, groupId, proxyName, objectName)
                    : new RaftSessionAwareSemaphoreProxy(nodeEngine, groupId, proxyName, objectName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
