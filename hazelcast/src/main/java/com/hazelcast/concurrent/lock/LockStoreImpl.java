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

import com.hazelcast.concurrent.lock.operations.AwaitOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LockStoreImpl implements DataSerializable, LockStore {

    private final transient ConstructorFunction<Data, LockResourceImpl> lockConstructor =
            new ConstructorFunction<Data, LockResourceImpl>() {
                @Override
                public LockResourceImpl createNew(Data key) {
                    return new LockResourceImpl(key, LockStoreImpl.this);
                }
            };

    private final ConcurrentMap<Data, LockResourceImpl> locks = new ConcurrentHashMap<Data, LockResourceImpl>();
    private ObjectNamespace namespace;
    private int backupCount;
    private int asyncBackupCount;

    private LockServiceImpl lockService;

    public LockStoreImpl() {
    }

    public LockStoreImpl(LockServiceImpl lockService, ObjectNamespace name, int backupCount, int asyncBackupCount) {
        this.namespace = name;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.lockService = lockService;
    }

    public boolean lock(Data key, String caller, long threadId) {
        return lock(key, caller, threadId, Long.MAX_VALUE);
    }

    @Override
    public boolean lock(Data key, String caller, long threadId, long leaseTime) {
        LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, leaseTime);
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long leaseTime) {
        LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, leaseTime, true);
    }

    @Override
    public boolean extendLeaseTime(Data key, String caller, long threadId, long leaseTime) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.extendLeaseTime(caller, threadId, leaseTime);
    }

    private LockResourceImpl getLock(Data key) {
        return ConcurrencyUtil.getOrPutIfAbsent(locks, key, lockConstructor);
    }

    @Override
    public boolean isLocked(Data key) {
        LockResource lock = locks.get(key);
        return lock != null && lock.isLocked();
    }

    @Override
    public boolean isLockedBy(Data key, String caller, long threadId) {
        LockResource lock = locks.get(key);
        if (lock == null) {
            return false;
        }
        return lock.isLockedBy(caller, threadId);
    }

    @Override
    public int getLockCount(Data key) {
        LockResource lock = locks.get(key);
        if (lock == null) {
            return 0;
        } else {
            return lock.getLockCount();
        }
    }

    @Override
    public long getRemainingLeaseTime(Data key) {
        LockResource lock = locks.get(key);
        if (lock == null) {
            return -1L;
        } else {
            return lock.getRemainingLeaseTime();
        }
    }

    @Override
    public boolean canAcquireLock(Data key, String caller, long threadId) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return true;
        } else {
            return lock.canAcquireLock(caller, threadId);
        }
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return false;
        }

        boolean result = false;
        if (lock.canAcquireLock(caller, threadId)) {
            if (lock.unlock(caller, threadId)) {
                result = true;
            }
        }
        if (lock.isRemovable()) {
            locks.remove(key);
        }
        return result;
    }

    @Override
    public boolean forceUnlock(Data key) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return false;
        } else {
            lock.clear();
            if (lock.isRemovable()) {
                locks.remove(key);
                lock.cancelEviction();
            }
            return true;
        }
    }

    public Collection<LockResource> getLocks() {
        return Collections.<LockResource>unmodifiableCollection(locks.values());
    }

    @Override
    public Set<Data> getLockedKeys() {
        Set<Data> keySet = new HashSet<Data>(locks.size());
        for (Map.Entry<Data, LockResourceImpl> entry : locks.entrySet()) {
            Data key = entry.getKey();
            LockResource lock = entry.getValue();
            if (lock.isLocked()) {
                keySet.add(key);
            }
        }
        return keySet;
    }

    void scheduleEviction(Data key, long leaseTime) {
        lockService.scheduleEviction(namespace, key, leaseTime);
    }

    void cancelEviction(Data key) {
        lockService.cancelEviction(namespace, key);
    }

    void setLockService(LockServiceImpl lockService) {
        this.lockService = lockService;
    }

    public void clear() {
        locks.clear();
    }

    public ObjectNamespace getNamespace() {
        return namespace;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public boolean addAwait(Data key, String conditionId, String caller, long threadId) {
        LockResourceImpl lock = getLock(key);
        return lock.addAwait(conditionId, caller, threadId);
    }

    public boolean removeAwait(Data key, String conditionId, String caller, long threadId) {
        LockResourceImpl lock = getLock(key);
        return lock.removeAwait(conditionId, caller, threadId);
    }

    public boolean startAwaiting(Data key, String conditionId, String caller, long threadId) {
        LockResourceImpl lock = getLock(key);
        return lock.startAwaiting(conditionId, caller, threadId);
    }

    public int getAwaitCount(Data key, String conditionId) {
        LockResourceImpl lock = getLock(key);
        return lock.getAwaitCount(conditionId);
    }

    public void registerSignalKey(ConditionKey conditionKey) {
        LockResourceImpl lock = getLock(conditionKey.getKey());
        lock.registerSignalKey(conditionKey);
    }

    public ConditionKey getSignalKey(Data key) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return null;
        } else {
            return lock.getSignalKey();
        }
    }

    public void removeSignalKey(ConditionKey conditionKey) {
        LockResourceImpl lock = locks.get(conditionKey.getKey());
        if (lock != null) {
            lock.removeSignalKey(conditionKey);
        }
    }

    public void registerExpiredAwaitOp(AwaitOperation awaitResponse) {
        Data key = awaitResponse.getKey();
        LockResourceImpl lock = getLock(key);
        lock.registerExpiredAwaitOp(awaitResponse);
    }

    public AwaitOperation pollExpiredAwaitOp(Data key) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return null;
        } else {
            return lock.pollExpiredAwaitOp();
        }
    }

    @Override
    public String getOwnerInfo(Data key) {
        final LockResource lock = locks.get(key);
        if (lock == null) {
            return "<not-locked>";
        } else {
            return "Owner: " + lock.getOwner() + ", thread-id: " + lock.getThreadId();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(namespace);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        int len = locks.size();
        out.writeInt(len);
        if (len > 0) {
            for (LockResourceImpl lock : locks.values()) {
                lock.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        namespace = in.readObject();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        int len = in.readInt();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                LockResourceImpl lock = new LockResourceImpl();
                lock.readData(in);
                lock.setLockStore(this);
                locks.put(lock.getKey(), lock);
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LockStoreImpl");
        sb.append("{namespace=").append(namespace);
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        return sb.toString();
    }
}
