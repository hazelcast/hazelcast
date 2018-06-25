/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.semaphore.proxy;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.ChangePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.DrainPermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.Clock;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Server-side session-aware proxy of Raft-based {@link ISemaphore} API
 */
public class RaftSessionAwareSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    /**
     * Since a proxy does not know how many permits will be drained on the Raft group,
     * it uses this constant to increment its local session acquire count.
     * Then, it adjusts the local session acquire count after the drain response is returned.
     */
    public static final int DRAIN_SESSION_ACQ_COUNT = 1024;

    private final String name;
    private final RaftInvocationManager invocationManager;

    public RaftSessionAwareSemaphoreProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager,
                                          RaftGroupId groupId, String name) {
        super(sessionManager, groupId);
        this.name = name;
        this.invocationManager = invocationManager;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");
        return invocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(name, permits)).join();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession(permits);
            RaftOp op = new AcquirePermitsOp(name, sessionId, threadId, invocationUid, permits, -1L);
            try {
                invocationManager.invoke(groupId, op).join();
                return;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
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
        checkPositive(permits, "Permits must be positive!");
        long timeoutMs = max(0, unit.toMillis(timeout));
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        long start;
        for (;;) {
            start = Clock.currentTimeMillis();
            long sessionId = acquireSession(permits);
            RaftOp op = new AcquirePermitsOp(name, sessionId, threadId, invocationUid, permits, timeoutMs);
            try {
                InternalCompletableFuture<Boolean> f = invocationManager.invoke(groupId, op);
                boolean acquired = f.join();
                if (!acquired) {
                    releaseSession(sessionId, permits);
                }
                return acquired;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                timeoutMs -= (Clock.currentTimeMillis() - start);
                if (timeoutMs <= 0) {
                    return false;
                }
            }
        }
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long sessionId = getSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        RaftOp op = new ReleasePermitsOp(name, sessionId, threadId, invocationUid, permits);
        try {
            invocationManager.invoke(groupId, op).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw e;
        } finally {
            releaseSession(sessionId, permits);
        }
    }

    @Override
    public int availablePermits() {
        return invocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)).join();
    }

    @Override
    public int drainPermits() {
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession(DRAIN_SESSION_ACQ_COUNT);
            RaftOp op = new DrainPermitsOp(name, sessionId, threadId, invocationUid);
            try {
                InternalCompletableFuture<Integer> future = invocationManager.invoke(groupId, op);
                int count = future.join();
                releaseSession(sessionId, DRAIN_SESSION_ACQ_COUNT - count);
                return count;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        long sessionId = acquireSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        try {
            invocationManager.invoke(groupId, new ChangePermitsOp(name, sessionId, threadId, invocationUid, -reduction)).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw e;
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        long sessionId = acquireSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        try {
            invocationManager.invoke(groupId, new ChangePermitsOp(name, sessionId, threadId, invocationUid, increase)).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw e;
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}
