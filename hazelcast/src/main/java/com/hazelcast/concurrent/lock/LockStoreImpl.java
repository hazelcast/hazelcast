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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class LockStoreImpl implements DataSerializable, LockStore {

    private transient final ConstructorFunction<Data, LockResourceImpl> lockConstructor = new ConstructorFunction<Data, LockResourceImpl>() {
        public LockResourceImpl createNew(Data key) {
            return new LockResourceImpl(key, LockStoreImpl.this);
        }
    };

    private final ConcurrentMap<Data, LockResourceImpl> locks = new ConcurrentHashMap<Data, LockResourceImpl>();
    private ObjectNamespace namespace;
    private int backupCount;
    private int asyncBackupCount;

    private transient LockServiceImpl lockService;

    public LockStoreImpl() {
    }

    public LockStoreImpl(LockServiceImpl lockService, ObjectNamespace name, int backupCount, int asyncBackupCount) {
        this.namespace = name;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.lockService = lockService;
    }

    public boolean lock(Data key, String caller, int threadId) {
        return lock(key, caller, threadId, Long.MAX_VALUE);
    }

    public boolean lock(Data key, String caller, int threadId, long leaseTime) {
        final LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, leaseTime);
    }

    public boolean txnLock(Data key, String caller, int threadId, long leaseTime) {
        final LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, leaseTime, true);
    }

    public boolean extendLeaseTime(Data key, String caller, int threadId, long leaseTime) {
        final LockResourceImpl lock = locks.get(key);
        return lock != null && lock.extendLeaseTime(caller, threadId, leaseTime);
    }

    private LockResourceImpl getLock(Data key) {
        return ConcurrencyUtil.getOrPutIfAbsent(locks, key, lockConstructor);
    }

    public boolean isLocked(Data key) {
        final LockResource lock = locks.get(key);
        return lock != null && lock.isLocked();
    }

    public boolean isLockedBy(Data key, String caller, int threadId) {
        LockResource lock = locks.get(key);
        return lock != null && lock.isLockedBy(caller, threadId);
    }

    public int getLockCount(Data key) {
        LockResource lock = locks.get(key);
        return lock != null ? lock.getLockCount() : 0;
    }

    public long getRemainingLeaseTime(Data key) {
        LockResource lock = locks.get(key);
        return lock != null ? lock.getRemainingLeaseTime() : -1L;
    }

    public boolean canAcquireLock(Data key, String caller, int threadId) {
        final LockResourceImpl lock = locks.get(key);
        return lock == null || lock.canAcquireLock(caller, threadId);
    }

    public boolean unlock(Data key, String caller, int threadId) {
        final LockResourceImpl lock = locks.get(key);
        boolean result = false;
        if (lock == null)
            return result;
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

    public boolean forceUnlock(Data key) {
        final LockResourceImpl lock = locks.get(key);
        if (lock == null)
            return false;
        else {
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

    public Set<Data> getLockedKeys() {
        Set<Data> keySet = new HashSet<Data>(locks.size());
        for (Map.Entry<Data, LockResourceImpl> entry : locks.entrySet()) {
            final Data key = entry.getKey();
            final LockResource lock = entry.getValue();
            if (lock.isLocked()){
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

    boolean addAwait(Data key, String conditionId, String caller, int threadId) {
        return getLock(key).addAwait(conditionId, caller, threadId);
    }

    boolean removeAwait(Data key, String conditionId, String caller, int threadId) {
        return getLock(key).removeAwait(conditionId, caller, threadId);
    }

    boolean startAwaiting(Data key, String conditionId, String caller, int threadId) {
        return getLock(key).startAwaiting(conditionId, caller, threadId);
    }

    int getAwaitCount(Data key, String conditionId) {
        return getLock(key).getAwaitCount(conditionId);
    }

    void registerSignalKey(ConditionKey conditionKey) {
        getLock(conditionKey.getKey()).registerSignalKey(conditionKey);
    }

    ConditionKey getSignalKey(Data key) {
        final LockResourceImpl lock = locks.get(key);
        return lock != null ? lock.getSignalKey() : null;
    }

    void removeSignalKey(ConditionKey conditionKey) {
        final LockResourceImpl lock = locks.get(conditionKey.getKey());
        if (lock != null) {
            lock.removeSignalKey(conditionKey);
        }
    }

    void registerExpiredAwaitOp(AwaitOperation awaitResponse) {
        final Data key = awaitResponse.getKey();
        getLock(key).registerExpiredAwaitOp(awaitResponse);
    }

    AwaitOperation pollExpiredAwaitOp(Data key) {
        LockResourceImpl lock = locks.get(key);
        return lock != null ? lock.pollExpiredAwaitOp() : null;
    }

    public String getOwnerInfo(Data dataKey) {
        final LockResource lock = locks.get(dataKey);
        return lock != null ? "Owner: " + lock.getOwner() + ", thread-id: " + lock.getThreadId()
                : "<not-locked>";
    }

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
