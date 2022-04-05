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

package com.hazelcast.cp.internal.datastructures.semaphore.proxy;

import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Server-side sessionless proxy of Raft-based {@link ISemaphore}
 */
public class SessionlessSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    private final RaftInvocationManager invocationManager;
    private final String proxyName;
    private final String objectName;

    public SessionlessSemaphoreProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        super(nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME), groupId);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");
        return invocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(objectName, permits)).joinInternal();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        RaftOp op = new AcquirePermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits, -1L);
        try {
            invocationManager.invoke(groupId, op).joinInternal();
        } catch (WaitKeyCancelledException e) {
            throw new IllegalStateException("Semaphore[" + objectName + "] not acquired because the acquire call "
                    + "on the CP group is cancelled, possibly because of another indeterminate call from the same thread.");
        }
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        checkPositive("permits", permits);
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        long timeoutMs = max(0, unit.toMillis(timeout));
        RaftOp op = new AcquirePermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits, timeoutMs);
        try {
            return invocationManager.<Boolean>invoke(groupId, op).joinInternal();
        } catch (WaitKeyCancelledException e) {
            throw new IllegalStateException("Semaphore[" + objectName + "] not acquired because the acquire call "
                    + "on the CP group is cancelled, possibly because of another indeterminate call from the same thread.");
        }
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        RaftOp op = new ReleasePermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), permits);
        invocationManager.invoke(groupId, op).joinInternal();
    }

    @Override
    public int availablePermits() {
        return invocationManager.<Integer>query(groupId, new AvailablePermitsOp(objectName), LINEARIZABLE).joinInternal();
    }

    @Override
    public int drainPermits() {
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        RaftOp op = new DrainPermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID());
        return invocationManager.<Integer>invoke(groupId, op).joinInternal();
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        RaftOp op = new ChangePermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), -reduction);
        invocationManager.invoke(groupId, op).joinInternal();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        long clusterWideThreadId = getOrCreateUniqueThreadId();
        RaftOp op = new ChangePermitsOp(objectName, NO_SESSION_ID, clusterWideThreadId, newUnsecureUUID(), increase);
        invocationManager.invoke(groupId, op).joinInternal();
    }

    @Override
    public String getName() {
        return proxyName;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).joinInternal();
    }

}
