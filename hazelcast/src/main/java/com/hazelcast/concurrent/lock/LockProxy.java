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

package com.hazelcast.concurrent.lock;

import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.util.ValidationUtil.shouldBePositive;

public class LockProxy extends AbstractDistributedObject<LockServiceImpl> implements ILock {

    private final String name;
    private final LockProxySupport lockSupport;
    private final Data key;
    private final int partitionId;

    public LockProxy(NodeEngine nodeEngine, LockServiceImpl lockService, String name) {
        super(nodeEngine, lockService);
        this.name = name;
        this.key = getNameAsPartitionAwareData();
        this.lockSupport = new LockProxySupport(new InternalLockNamespace(name));
        this.partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
    }

    @Override
    public boolean isLocked() {
        return lockSupport.isLocked(getNodeEngine(), key);
    }

    @Override
    public boolean isLockedByCurrentThread() {
        return lockSupport.isLockedByCurrentThread(getNodeEngine(), key);
    }

    @Override
    public int getLockCount() {
        return lockSupport.getLockCount(getNodeEngine(), key);
    }

    @Override
    public long getRemainingLeaseTime() {
        return lockSupport.getRemainingLeaseTime(getNodeEngine(), key);
    }

    @Override
    public void lock() {
        lockSupport.lock(getNodeEngine(), key);
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        shouldBePositive(leaseTime, "leaseTime");
        lockSupport.lock(getNodeEngine(), key, timeUnit.toMillis(leaseTime));
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    @Override
    public boolean tryLock() {
        return lockSupport.tryLock(getNodeEngine(), key);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("unit can't be null");
        }

        return lockSupport.tryLock(getNodeEngine(), key, time, unit);
    }

    @Override
    public void unlock() {
        lockSupport.unlock(getNodeEngine(), key);
    }

    @Override
    public void forceUnlock() {
        lockSupport.forceUnlock(getNodeEngine(), key);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Use ICondition.newCondition(String name) instead!");
    }

    @Override
    public ICondition newCondition(String name) {
        if (name == null) {
            throw new NullPointerException("Condition name can't be null");
        }
        return new ConditionImpl(this, name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Deprecated
    public Object getKey() {
        return getName();
    }

    public Data getKeyData() {
        return key;
    }

    public int getPartitionId() {
        return partitionId;
    }

    ObjectNamespace getNamespace() {
        return lockSupport.getNamespace();
    }

    // will be removed when HazelcastInstance.getLock(Object key) is removed from API
    public static String convertToStringKey(Object key, SerializationService serializationService) {
        if (key instanceof String) {
            return String.valueOf(key);
        } else {
            Data data = serializationService.toData(key, PARTITIONING_STRATEGY);
//            name = Integer.toString(data.hashCode());
            byte[] buffer = data.getBuffer();
            return Arrays.toString(buffer);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ILock{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
