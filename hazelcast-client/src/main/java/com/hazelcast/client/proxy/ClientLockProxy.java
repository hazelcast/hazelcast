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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientLockReferenceIdGenerator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.LockForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetLockCountCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetRemainingLeaseTimeCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedByCurrentThreadCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.LockLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockUnlockCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link ILock}.
 */
public class ClientLockProxy extends PartitionSpecificClientProxy implements ILock {

    private ClientLockReferenceIdGenerator referenceIdGenerator;

    public ClientLockProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Deprecated
    public Object getKey() {
        return name;
    }

    @Override
    public boolean isLocked() {
        ClientMessage request = LockIsLockedCodec.encodeRequest(name);
        LockIsLockedCodec.ResponseParameters resultParameters =
                LockIsLockedCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        ClientMessage request = LockIsLockedByCurrentThreadCodec.encodeRequest(name, ThreadUtil.getThreadId());
        LockIsLockedByCurrentThreadCodec.ResponseParameters resultParameters =
                LockIsLockedByCurrentThreadCodec.decodeResponse(invokeOnPartition(request));

        return resultParameters.response;
    }

    @Override
    public int getLockCount() {
        ClientMessage request = LockGetLockCountCodec.encodeRequest(name);
        LockGetLockCountCodec.ResponseParameters resultParameters
                = LockGetLockCountCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public long getRemainingLeaseTime() {
        ClientMessage request = LockGetRemainingLeaseTimeCodec.encodeRequest(name);
        LockGetRemainingLeaseTimeCodec.ResponseParameters resultParameters =
                LockGetRemainingLeaseTimeCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        checkPositive(leaseTime, "leaseTime should be positive");
        ClientMessage request = LockLockCodec.encodeRequest(name,
                getTimeInMillis(leaseTime, timeUnit), ThreadUtil.getThreadId(), referenceIdGenerator.getNextReferenceId());
        invokeOnPartition(request);
    }

    @Override
    public void forceUnlock() {
        ClientMessage request = LockForceUnlockCodec.encodeRequest(name, referenceIdGenerator.getNextReferenceId());
        invokeOnPartition(request);
    }

    @Override
    public ICondition newCondition(String name) {
        checkNotNull(name, "Condition name can't be null");
        ClientConditionProxy clientConditionProxy = new ClientConditionProxy(this, name, getContext());
        clientConditionProxy.onInitialize();
        return clientConditionProxy;
    }

    @Override
    public void lock() {
        ClientMessage request = LockLockCodec
                .encodeRequest(name, -1, ThreadUtil.getThreadId(), referenceIdGenerator.getNextReferenceId());
        invokeOnPartition(request);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        ClientMessage request = LockLockCodec
                .encodeRequest(name, -1, ThreadUtil.getThreadId(), referenceIdGenerator.getNextReferenceId());
        invokeOnPartitionInterruptibly(request);
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(0, null);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        return tryLock(timeout, unit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        long timeoutInMillis = getTimeInMillis(timeout, unit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseUnit);
        long threadId = ThreadUtil.getThreadId();
        ClientMessage request = LockTryLockCodec
                .encodeRequest(name, threadId, leaseTimeInMillis, timeoutInMillis, referenceIdGenerator.getNextReferenceId());
        LockTryLockCodec.ResponseParameters resultParameters = LockTryLockCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    @Override
    public void unlock() {
        ClientMessage request = LockUnlockCodec
                .encodeRequest(name, ThreadUtil.getThreadId(), referenceIdGenerator.getNextReferenceId());
        invokeOnPartition(request);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    @Override
    public String toString() {
        return "ILock{" + "name='" + name + '\'' + '}';
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        referenceIdGenerator = getClient().getLockReferenceIdGenerator();
    }
}
