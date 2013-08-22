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

package com.hazelcast.concurrent.lock.proxy;

import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.PartitionKeyUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @author mdogan 2/12/13
 */
public class LockProxy extends AbstractDistributedObject<LockServiceImpl> implements ILock {

    final Data key;
    final Data partitionKey;
    final InternalLockNamespace namespace = new InternalLockNamespace();
    private final LockProxySupport lockSupport;

    public LockProxy(NodeEngine nodeEngine, LockServiceImpl lockService, final Object actualKey) {
        super(nodeEngine, lockService);
        key = nodeEngine.toData(actualKey);
        Object _partitionKey = PartitionKeyUtil.getPartitionKey(actualKey);
        partitionKey = _partitionKey == actualKey ? key : nodeEngine.toData(_partitionKey);
        lockSupport = new LockProxySupport(namespace);
    }

    public boolean isLocked() {
        return lockSupport.isLocked(getNodeEngine(), key, partitionKey);
    }

    public boolean isLockedByCurrentThread() {
        return lockSupport.isLockedByCurrentThread(getNodeEngine(), key, partitionKey);
    }

    public int getLockCount() {
        return lockSupport.getLockCount(getNodeEngine(), key, partitionKey);
    }

    public long getRemainingLeaseTime() {
        return lockSupport.getRemainingLeaseTime(getNodeEngine(), key, partitionKey);
    }

    public void lock() {
        lockSupport.lock(getNodeEngine(), key, partitionKey);
    }

    public void lock(long ttl, TimeUnit timeUnit) {
        lockSupport.lock(getNodeEngine(), key, partitionKey, timeUnit.toMillis(ttl));
    }

    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    public boolean tryLock() {
        return lockSupport.tryLock(getNodeEngine(), key, partitionKey);
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lockSupport.tryLock(getNodeEngine(), key, time, unit, partitionKey);
    }

    public void unlock() {
        lockSupport.unlock(getNodeEngine(), key, partitionKey);
    }

    public void forceUnlock() {
        lockSupport.forceUnlock(getNodeEngine(), key, partitionKey);
    }

    public Condition newCondition() {
        throw new UnsupportedOperationException("Use ICondition.newCondition(String name) instead!");
    }

    public ICondition newCondition(String name) {
        return new ConditionImpl(this, name);
    }

    public Object getId() {
        return key;
    }

    public String getName() {
        return String.valueOf(getKey());
    }

    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    public Object getKey() {
        return getNodeEngine().toObject(key);
    }
}
