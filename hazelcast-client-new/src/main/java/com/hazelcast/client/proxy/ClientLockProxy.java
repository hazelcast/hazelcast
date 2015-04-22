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

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.*;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.lock.client.GetLockCountRequest;
import com.hazelcast.concurrent.lock.client.GetRemainingLeaseRequest;
import com.hazelcast.concurrent.lock.client.IsLockedRequest;
import com.hazelcast.concurrent.lock.client.LockRequest;
import com.hazelcast.concurrent.lock.client.UnlockRequest;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ThreadUtil;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.ValidationUtil.shouldBePositive;

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
        ClientMessage request = LockIsLockedParameters.encode(getName(), ThreadUtil.getThreadId());
        BooleanResultParameters resultParameters =
                BooleanResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;
    }

    public boolean isLockedByCurrentThread() {
        ClientMessage request = LockIsLockedByCurrentThreadParameters.encode(getName(), ThreadUtil.getThreadId());
        BooleanResultParameters resultParameters =
                BooleanResultParameters.decode((ClientMessage) invoke(request));

        return resultParameters.result;
    }

    public int getLockCount() {
        ClientMessage request = LockGetLockCountParameters.encode(getName());
        IntResultParameters resultParameters = IntResultParameters.decode((ClientMessage) invoke(request));
        return resultParameters.result;
    }

    public long getRemainingLeaseTime() {
        ClientMessage request = LockGetRemainingLeaseTimeParameters.encode(getName());
        LongResultParameters resultParameters = LongResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;
    }

    public void lock(long leaseTime, TimeUnit timeUnit) {
        shouldBePositive(leaseTime, "leaseTime");
        ClientMessage request = LockLockParameters.encode(getName(), getTimeInMillis(leaseTime, timeUnit), ThreadUtil.getThreadId());
        invoke(request);
    }

    public void forceUnlock() {
        ClientMessage request = LockForceUnlockParameters.encode(getName());
        invoke(request);
    }

    public ICondition newCondition(String name) {
        return new ClientConditionProxy(this, name, getContext());
    }

    public void lock() {
        lock(Long.MAX_VALUE, null);
    }

    public void lockInterruptibly() throws InterruptedException {
        ClientMessage request = LockLockParameters.encode(getName(), Long.MAX_VALUE, ThreadUtil.getThreadId());
        invokeInterruptibly(request, getKeyData());
    }

    public boolean tryLock() {
        try {
            return tryLock(0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        ClientMessage request = LockTryLockParameters.encode(getName(), ThreadUtil.getThreadId(), getTimeInMillis(time, unit));
        BooleanResultParameters resultParameters = BooleanResultParameters.decode((ClientMessage)invoke(request));
        return resultParameters.result;
    }

    public void unlock() {
        ClientMessage request = LockUnlockParameters.encode(getName(), ThreadUtil.getThreadId());
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

    protected <T> T invoke(ClientRequest req) {
        return super.invoke(req, getKeyData());
    }

    @Override
    public String toString() {
        return "ILock{" + "name='" + getName() + '\'' + '}';
    }
}
