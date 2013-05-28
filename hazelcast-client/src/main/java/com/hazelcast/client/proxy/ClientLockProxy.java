/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.concurrent.lock.client.IsLockedRequest;
import com.hazelcast.concurrent.lock.client.LockDestroyRequest;
import com.hazelcast.concurrent.lock.client.LockRequest;
import com.hazelcast.concurrent.lock.client.UnlockRequest;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @ali 5/28/13
 */
public class ClientLockProxy extends ClientProxy implements ILock {

    private Data key;

    public ClientLockProxy(String serviceName, Object objectId) {
        super(serviceName, objectId);
    }

    public Object getKey() {
        return getId();
    }

    public LocalLockStats getLocalLockStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public boolean isLocked() {
        IsLockedRequest request = new IsLockedRequest(getKeyData());
        Boolean result = invoke(request);
        return result;
    }

    public void lock(long leaseTime, TimeUnit timeUnit) {
        LockRequest request = new LockRequest(getKeyData(), ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request);
    }

    public void forceUnlock() {
        UnlockRequest request = new UnlockRequest(getKeyData(), ThreadUtil.getThreadId(), true);
        invoke(request);
    }

    public ICondition newCondition(String name) {
        throw new UnsupportedOperationException();
    }

    public void lock() {
        lock(-1, null);
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
        LockRequest request = new LockRequest(getKeyData(), ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, unit));
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

    protected void onDestroy() {
        LockDestroyRequest request = new LockDestroyRequest(getKeyData());
        invoke(request);
    }

    public String getName() {
        return String.valueOf(getId());
    }

    private Data toData(Object o){
        return getContext().getSerializationService().toData(o);
    }

    private Data getKeyData(){
        if (key == null){
            key = toData(getId());
        }
        return key;
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKeyData());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
