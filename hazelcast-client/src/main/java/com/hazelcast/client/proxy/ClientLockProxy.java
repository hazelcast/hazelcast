/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.LockForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetLockCountCodec;
import com.hazelcast.client.impl.protocol.codec.LockGetRemainingLeaseTimeCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedByCurrentThreadCodec;
import com.hazelcast.client.impl.protocol.codec.LockIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.LockLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.LockUnlockCodec;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Proxy implementation of {@link ILock}.
 */
public class ClientLockProxy extends PartitionSpecificClientProxy implements ILock {

    public ClientLockProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Deprecated
    public Object getKey() {
        return name;
    }

    public boolean isLocked() {
        ClientMessage request = LockIsLockedCodec.encodeRequest(name);
        LockIsLockedCodec.ResponseParameters resultParameters =
                LockIsLockedCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    public boolean isLockedByCurrentThread() {
        ClientMessage request = LockIsLockedByCurrentThreadCodec.encodeRequest(name, ThreadUtil.getThreadId());
        LockIsLockedByCurrentThreadCodec.ResponseParameters resultParameters =
                LockIsLockedByCurrentThreadCodec.decodeResponse(invokeOnPartition(request));

        return resultParameters.response;
    }

    public int getLockCount() {
        ClientMessage request = LockGetLockCountCodec.encodeRequest(name);
        LockGetLockCountCodec.ResponseParameters resultParameters
                = LockGetLockCountCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    public long getRemainingLeaseTime() {
        ClientMessage request = LockGetRemainingLeaseTimeCodec.encodeRequest(name);
        LockGetRemainingLeaseTimeCodec.ResponseParameters resultParameters =
                LockGetRemainingLeaseTimeCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    public void lock(long leaseTime, TimeUnit timeUnit) {
        checkPositive(leaseTime, "leaseTime should be positive");
        ClientMessage request = LockLockCodec.encodeRequest(name,
                getTimeInMillis(leaseTime, timeUnit), ThreadUtil.getThreadId());
        invokeOnPartition(request);
    }

    public void forceUnlock() {
        ClientMessage request = LockForceUnlockCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    public ICondition newCondition(String name) {
        ClientConditionProxy clientConditionProxy = new ClientConditionProxy(this, name, getContext());
        clientConditionProxy.onInitialize();
        return clientConditionProxy;
    }

    public void lock() {
        ClientMessage request = LockLockCodec.encodeRequest(name, -1, ThreadUtil.getThreadId());
        invokeOnPartition(request);
    }

    public void lockInterruptibly() throws InterruptedException {
        ClientMessage request = LockLockCodec.encodeRequest(name, -1, ThreadUtil.getThreadId());
        invokeOnPartitionInterruptibly(request);
    }

    public boolean tryLock() {
        try {
            return tryLock(0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        return tryLock(timeout, unit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        long timeoutInMillis = getTimeInMillis(timeout, unit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseUnit);
        long threadId = ThreadUtil.getThreadId();
        ClientMessage request = LockTryLockCodec.encodeRequest(name, threadId, leaseTimeInMillis, timeoutInMillis);
        LockTryLockCodec.ResponseParameters resultParameters = LockTryLockCodec.decodeResponse(invokeOnPartition(request));
        return resultParameters.response;
    }

    public void unlock() {
        ClientMessage request = LockUnlockCodec.encodeRequest(name, ThreadUtil.getThreadId());
        invokeOnPartition(request);
    }

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
}
