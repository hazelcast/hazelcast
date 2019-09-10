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

package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreReleaseCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.util.Clock;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy.DRAIN_SESSION_ACQ_COUNT;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Client-side session-aware proxy of Raft-based {@link ISemaphore}
 */
public class RaftSessionAwareSemaphoreProxy extends ClientProxy implements ISemaphore {

    private final ClientProxySessionManager sessionManager;
    private final RaftGroupId groupId;
    private final String objectName;

    public RaftSessionAwareSemaphoreProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(RaftSemaphoreService.SERVICE_NAME, proxyName, context);
        this.sessionManager = getClient().getProxySessionManager();
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");

        ClientMessage request = CPSemaphoreInitCodec.encodeRequest(groupId, objectName, permits);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreInitCodec.decodeResponse(response).response;
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
            long sessionId = sessionManager.acquireSession(this.groupId, permits);
            try {
                ClientMessage request = CPSemaphoreAcquireCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                        invocationUid, permits, -1);
                HazelcastClientInstanceImpl client = getClient();
                new ClientInvocation(client, request, objectName).invoke().join();
                return;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(this.groupId, sessionId);
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
            long sessionId = sessionManager.acquireSession(this.groupId, permits);
            try {
                ClientMessage request = CPSemaphoreAcquireCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                        invocationUid, permits, timeoutMs);
                HazelcastClientInstanceImpl client = getClient();
                ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
                boolean acquired = CPSemaphoreAcquireCodec.decodeResponse(response).response;
                if (!acquired) {
                    sessionManager.releaseSession(this.groupId, sessionId, permits);
                }
                return acquired;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(this.groupId, sessionId);
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
        long sessionId = sessionManager.getSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw newIllegalStateException(null);
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        try {
            ClientMessage request = CPSemaphoreReleaseCodec.encodeRequest(groupId, objectName, sessionId, threadId, invocationUid,
                    permits);
            HazelcastClientInstanceImpl client = getClient();
            new ClientInvocation(client, request, objectName).invoke().join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(this.groupId, sessionId);
            throw newIllegalStateException(e);
        } finally {
            sessionManager.releaseSession(this.groupId, sessionId, permits);
        }
    }

    @Override
    public int availablePermits() {
        ClientMessage request = CPSemaphoreAvailablePermitsCodec.encodeRequest(groupId, objectName);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreAvailablePermitsCodec.decodeResponse(response).response;
    }

    @Override
    public int drainPermits() {
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        for (;;) {
            long sessionId = sessionManager.acquireSession(this.groupId, DRAIN_SESSION_ACQ_COUNT);
            try {
                ClientMessage request = CPSemaphoreDrainCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                        invocationUid);
                HazelcastClientInstanceImpl client = getClient();
                ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
                return CPSemaphoreDrainCodec.decodeResponse(response).response;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(this.groupId, sessionId);
            }
        }
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }

        long sessionId = sessionManager.acquireSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw newIllegalStateException(null);
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        try {
            ClientMessage request = CPSemaphoreChangeCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                    invocationUid, -reduction);
            new ClientInvocation(getClient(), request, objectName).invoke().join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(this.groupId, sessionId);
            throw newIllegalStateException(e);
        } finally {
            sessionManager.releaseSession(this.groupId, sessionId);
        }
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }

        long sessionId = sessionManager.acquireSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw newIllegalStateException(null);
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        try {
            ClientMessage request = CPSemaphoreChangeCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                    invocationUid, increase);
            new ClientInvocation(getClient(), request, objectName).invoke().join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(this.groupId, sessionId);
            throw newIllegalStateException(e);
        } finally {
            sessionManager.releaseSession(this.groupId, sessionId);
        }
    }

    private IllegalStateException newIllegalStateException(SessionExpiredException e) {
        return new IllegalStateException("No valid session!", e);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().join();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

}
