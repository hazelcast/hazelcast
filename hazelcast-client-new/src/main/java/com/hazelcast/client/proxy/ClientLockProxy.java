/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.Preconditions.checkPositive;

public class ClientLockProxy extends ClientProxy implements ILock {

    private volatile Data key;

    public ClientLockProxy(String serviceName, String objectId) {
        super(serviceName, objectId);
    }

    @Deprecated
    public Object getKey() {
        return getName();
    }

    public boolean isLocked() {
        ClientMessage request = LockIsLockedCodec.encodeRequest(getName());
        LockIsLockedCodec.ResponseParameters resultParameters =
                LockIsLockedCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    public boolean isLockedByCurrentThread() {
        ClientMessage request = LockIsLockedByCurrentThreadCodec.encodeRequest(getName(), ThreadUtil.getThreadId());
        LockIsLockedByCurrentThreadCodec.ResponseParameters resultParameters =
                LockIsLockedByCurrentThreadCodec.decodeResponse((ClientMessage) invoke(request));

        return resultParameters.response;
    }

    public int getLockCount() {
        ClientMessage request = LockGetLockCountCodec.encodeRequest(getName());
        LockGetLockCountCodec.ResponseParameters resultParameters
                = LockGetLockCountCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    public long getRemainingLeaseTime() {
        ClientMessage request = LockGetRemainingLeaseTimeCodec.encodeRequest(getName());
        LockGetRemainingLeaseTimeCodec.ResponseParameters resultParameters =
                LockGetRemainingLeaseTimeCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    public void lock(long leaseTime, TimeUnit timeUnit) {
        checkPositive(leaseTime, "leaseTime should be positive");
        ClientMessage request = LockLockCodec.encodeRequest(getName(),
                getTimeInMillis(leaseTime, timeUnit), ThreadUtil.getThreadId());
        invoke(request);
    }

    public void forceUnlock() {
        ClientMessage request = LockForceUnlockCodec.encodeRequest(getName());
        invoke(request);
    }

    public ICondition newCondition(String name) {
        return new ClientConditionProxy(this, name, getContext());
    }

    public void lock() {
        ClientMessage request = LockLockCodec.encodeRequest(getName(), -1, ThreadUtil.getThreadId());
        invoke(request);
    }

    public void lockInterruptibly() throws InterruptedException {
        ClientMessage request = LockLockCodec.encodeRequest(getName(), -1, ThreadUtil.getThreadId());
        invokeInterruptibly(request, getKeyData());
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
        ClientMessage request = LockTryLockCodec.encodeRequest(getName(), threadId, leaseTimeInMillis, timeoutInMillis);
        LockTryLockCodec.ResponseParameters resultParameters = LockTryLockCodec.decodeResponse((ClientMessage) invoke(request));
        return resultParameters.response;
    }

    public void unlock() {
        ClientMessage request = LockUnlockCodec.encodeRequest(getName(), ThreadUtil.getThreadId());
        invoke(request);
    }

    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private Data getKeyData() {
        if (key == null) {
            key = toData(getName());
        }
        return key;
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getKeyData());
    }

    @Override
    public String toString() {
        return "ILock{" + "name='" + getName() + '\'' + '}';
    }
}
