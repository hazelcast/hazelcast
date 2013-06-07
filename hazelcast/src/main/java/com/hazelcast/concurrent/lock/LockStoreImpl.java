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

//TODO Possible leak because of empty lock objects
public class LockStoreImpl implements DataSerializable, LockStore {

    private transient final ConstructorFunction<Data, DistributedLock> lockConstructor
            = new ConstructorFunction<Data, DistributedLock>() {
        public DistributedLock createNew(Data key) {
            return new DistributedLock(key, lockService, namespace);
        }
    };

    private final ConcurrentMap<Data, DistributedLock> locks = new ConcurrentHashMap<Data, DistributedLock>();
    private ObjectNamespace namespace;
    private int backupCount;
    private int asyncBackupCount;
    private transient LockService lockService;

    public LockStoreImpl() {
    }

    public LockStoreImpl(ObjectNamespace name, int backupCount, int asyncBackupCount, LockService lockService) {
        this.namespace = name;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.lockService = lockService;
    }

    public boolean lock(Data key, String caller, int threadId) {
        return lock(key, caller, threadId, Long.MAX_VALUE);
    }

    public boolean lock(Data key, String caller, int threadId, long ttl) {
        final DistributedLock lock = getLock(key);
        return lock.lock(caller, threadId, ttl);
    }

    public boolean txnLock(Data key, String caller, int threadId, long ttl) {
        final DistributedLock lock = getLock(key);
        return lock.lock(caller, threadId, ttl, true);
    }

    public boolean extendTTL(Data key, String caller, int threadId, long ttl) {
        final DistributedLock lock = locks.get(key);
        return lock != null && lock.extendTTL(caller, threadId, ttl);
    }

    private DistributedLock getLock(Data key) {
        return ConcurrencyUtil.getOrPutIfAbsent(locks, key, lockConstructor);
    }

    public boolean isLocked(Data key) {
        final DistributedLock lock = locks.get(key);
        return lock != null && lock.isLocked();
    }

    public boolean isLockedBy(Data key, String caller, int threadId) {
        DistributedLock lock = locks.get(key);
        return lock != null && lock.isLockedBy(caller, threadId);
    }

    public boolean canAcquireLock(Data key, String caller, int threadId) {
        final DistributedLock lock = locks.get(key);
        return lock == null || lock.canAcquireLock(caller, threadId);
    }

    public boolean unlock(Data key, String caller, int threadId) {
        final DistributedLock lock = locks.get(key);
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
        final DistributedLock lock = locks.get(key);
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

    public Map<Data, DistributedLock> getLocks() {
        return Collections.unmodifiableMap(locks);
    }

    public Set<Data> getLockedKeys() {
        Set<Data> keySet = new HashSet<Data>(locks.size());
        for (Map.Entry<Data, DistributedLock> entry : locks.entrySet()) {
            final Data key = entry.getKey();
            final DistributedLock lock = entry.getValue();
            if (lock.isLocked()){
                keySet.add(key);
            }
        }
        return keySet;
    }

    public void setLockService(LockService lockService) {
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
        return getLock(key).getSignalKey();
    }

    void removeSignalKey(ConditionKey conditionKey) {
        getLock(conditionKey.getKey()).removeSignalKey(conditionKey);
    }

    void registerExpiredAwaitOp(AwaitOperation awaitResponse) {
        final Data key = awaitResponse.getKey();
        getLock(key).registerExpiredAwaitOp(awaitResponse);
    }

    AwaitOperation pollExpiredAwaitOp(Data key) {
        return getLock(key).pollExpiredAwaitOp();
    }

    public String getLockOwnerString(Data dataKey) {
        final DistributedLock lock = locks.get(dataKey);
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
            for (DistributedLock lock : locks.values()) {
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
                DistributedLock lock = new DistributedLock();
                lock.readData(in);
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
