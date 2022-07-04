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

package com.hazelcast.client.cp.internal.datastructures.lock;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.lock.proxy.AbstractFencedLockProxy;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Client-side proxy of Raft-based {@link FencedLock} API
 */
public class FencedLockProxy extends ClientProxy implements FencedLock {

    private static final ClientMessageDecoder GET_LOCK_OWNERSHIP_STATE_RESPONSE_DECODER = clientMessage -> {
        FencedLockGetLockOwnershipCodec.ResponseParameters params = FencedLockGetLockOwnershipCodec
                .decodeResponse(clientMessage);
        return new LockOwnershipState(params.fence, params.lockCount, params.sessionId, params.threadId);
    };

    private final FencedLockImpl lock;

    public FencedLockProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(LockService.SERVICE_NAME, proxyName, context);
        this.lock = new FencedLockImpl(getClient().getProxySessionManager(), groupId, proxyName, objectName);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public long lockAndGetFence() {
        return lock.lockAndGetFence();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public long tryLockAndGetFence() {
        return lock.tryLockAndGetFence();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        return lock.tryLock(time, unit);
    }

    @Override
    public long tryLockAndGetFence(long time, TimeUnit unit) {
        return lock.tryLockAndGetFence(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public long getFence() {
        return lock.getFence();
    }

    @Override
    public boolean isLocked() {
        return lock.isLocked();
    }

    @Override
    public boolean isLockedByCurrentThread() {
        return lock.isLockedByCurrentThread();
    }

    @Override
    public int getLockCount() {
        return lock.getLockCount();
    }

    @Override
    public CPGroupId getGroupId() {
        return lock.getGroupId();
    }

    @Override
    public Condition newCondition() {
        return lock.newCondition();
    }

    @Override
    public void onDestroy() {
        ClientMessage msg = CPGroupDestroyCPObjectCodec.encodeRequest(lock.getGroupId(), getServiceName(), lock.getObjectName());
        new ClientInvocation(getClient(), msg, name).invoke().joinInternal();
    }

    @Override
    protected void postDestroy() {
        super.postDestroy();
        lock.destroy();
    }


    private class FencedLockImpl extends AbstractFencedLockProxy {
        FencedLockImpl(AbstractProxySessionManager sessionManager, RaftGroupId groupId, String proxyName, String objectName) {
            super(sessionManager, groupId, proxyName, objectName);
        }

        @Override
        protected InternalCompletableFuture<Long> doLock(long sessionId, long threadId, UUID invocationUid) {
            ClientMessage request = FencedLockLockCodec.encodeRequest(groupId, objectName, sessionId, threadId, invocationUid);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), FencedLockLockCodec::decodeResponse);
        }

        @Override
        protected InternalCompletableFuture<Long> doTryLock(long sessionId, long threadId, UUID invocationUid,
                                                            long timeoutMillis) {
            ClientMessage request = FencedLockTryLockCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                    invocationUid, timeoutMillis);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), FencedLockTryLockCodec::decodeResponse);
        }

        @Override
        protected InternalCompletableFuture<Boolean> doUnlock(long sessionId, long threadId, UUID invocationUid) {
            ClientMessage request = FencedLockUnlockCodec.encodeRequest(groupId, objectName, sessionId, threadId,
                    invocationUid);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), FencedLockUnlockCodec::decodeResponse);
        }

        @Override
        protected InternalCompletableFuture<LockOwnershipState> doGetLockOwnershipState() {
            ClientMessage request = FencedLockGetLockOwnershipCodec.encodeRequest(groupId, objectName);
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), GET_LOCK_OWNERSHIP_STATE_RESPONSE_DECODER);
        }
    }

}
