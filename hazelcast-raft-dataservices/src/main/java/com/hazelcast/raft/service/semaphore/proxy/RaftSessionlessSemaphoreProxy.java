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
import com.hazelcast.raft.service.atomiclong.operation.AddAndGetOp;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.ChangePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.DrainPermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.semaphore.proxy.GloballyUniqueThreadIdUtil.GLOBAL_THREAD_ID_GENERATOR_NAME;
import static com.hazelcast.raft.service.semaphore.proxy.GloballyUniqueThreadIdUtil.getGlobalThreadId;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Server-side sessionless proxy of Raft-based {@link ISemaphore} API
 */
public class RaftSessionlessSemaphoreProxy implements ISemaphore {

    private final RaftInvocationManager invocationManager;
    private final RaftGroupId groupId;
    private final String name;

    /**
     * Since sessionId will be {@link AbstractSessionManager#NO_SESSION_ID} for all requests,
     * uniqueness of <sessionId, threadId> pair is provided via generating globally unique thread ids using a RaftAtomicLong.
     */
    private final ConstructorFunction<RaftGroupId, Long> globallyUniqueThreadIdCtor;

    public RaftSessionlessSemaphoreProxy(final RaftInvocationManager invocationManager, RaftGroupId groupId, String name) {
        this.invocationManager = invocationManager;
        this.groupId = groupId;
        this.name = name;
        this.globallyUniqueThreadIdCtor = new ConstructorFunction<RaftGroupId, Long>() {
            @Override
            public Long createNew(RaftGroupId groupId) {
                InternalCompletableFuture<Long> f = invocationManager
                        .invoke(groupId, new AddAndGetOp(GLOBAL_THREAD_ID_GENERATOR_NAME, 1));
                return f.join();
            }
        };
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
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        RaftOp op = new AcquirePermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID(), permits, -1L);
        invocationManager.invoke(groupId, op).join();
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
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        long timeoutMs = max(0, unit.toMillis(timeout));
        RaftOp op = new AcquirePermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID(), permits, timeoutMs);
        return invocationManager.<Boolean>invoke(groupId, op).join();
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        RaftOp op = new ReleasePermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID(), permits);
        invocationManager.invoke(groupId, op).join();
    }

    @Override
    public int availablePermits() {
        return invocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)).join();
    }

    @Override
    public int drainPermits() {
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        RaftOp op = new DrainPermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID());
        return invocationManager.<Integer>invoke(groupId, op).join();
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        RaftOp op = new ChangePermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID(), -reduction);
        invocationManager.invoke(groupId, op).join();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        long globalThreadId = getGlobalThreadId(groupId, globallyUniqueThreadIdCtor);
        RaftOp op = new ChangePermitsOp(name, NO_SESSION_ID, globalThreadId, newUnsecureUUID(), increase);
        invocationManager.invoke(groupId, op).join();
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

    public final RaftGroupId getGroupId() {
        return groupId;
    }

}
