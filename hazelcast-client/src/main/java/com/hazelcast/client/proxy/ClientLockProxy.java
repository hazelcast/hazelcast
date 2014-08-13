/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.lock.client.IsLockedRequest;
import com.hazelcast.concurrent.lock.client.GetLockCountRequest;
import com.hazelcast.concurrent.lock.client.GetRemainingLeaseRequest;
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

    public ClientLockProxy(String instanceName, String serviceName, String objectId) {
        super(instanceName, serviceName, objectId);
    }

    @Deprecated
    public Object getKey() {
        return getName();
    }

    public boolean isLocked() {
        IsLockedRequest request = new IsLockedRequest(getKeyData());
        Boolean result = invoke(request);
        return result;
    }

    public boolean isLockedByCurrentThread() {
        IsLockedRequest request = new IsLockedRequest(getKeyData(), ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    public int getLockCount() {
        GetLockCountRequest request = new GetLockCountRequest(getKeyData());
        return (Integer) invoke(request);
    }

    public long getRemainingLeaseTime() {
        GetRemainingLeaseRequest request = new GetRemainingLeaseRequest(getKeyData());
        return (Long) invoke(request);
    }

    public void lock(long leaseTime, TimeUnit timeUnit) {
        shouldBePositive(leaseTime, "leaseTime");
        LockRequest request = new LockRequest(getKeyData(), ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request);
    }

    public void forceUnlock() {
        UnlockRequest request = new UnlockRequest(getKeyData(), ThreadUtil.getThreadId(), true);
        invoke(request);
    }

    public ICondition newCondition(String name) {
        return new ClientConditionProxy(instanceName, this, name, getContext());
    }

    public void lock() {
        lock(Long.MAX_VALUE, null);
    }

    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    public boolean tryLock() {
        try {
            return tryLock(0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        LockRequest request = new LockRequest(getKeyData(),
                ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, unit));
        Boolean result = invoke(request);
        return result;
    }

    public void unlock() {
        UnlockRequest request = new UnlockRequest(getKeyData(), ThreadUtil.getThreadId());
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
