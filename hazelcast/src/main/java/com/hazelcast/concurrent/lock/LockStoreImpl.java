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
import com.hazelcast.util.ConcurrencyUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class LockStoreImpl implements DataSerializable, LockStore {

    private final ConcurrencyUtil.ConstructorFunction<Data, LockInfo> lockConstructor
            = new ConcurrencyUtil.ConstructorFunction<Data, LockInfo>() {
        public LockInfo createNew(Data key) {
            return new LockInfo(key);
        }
    };

    private final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>();

    private ILockNamespace namespace;
    private int backupCount;
    private int asyncBackupCount;

    public LockStoreImpl() {
    }

    public LockStoreImpl(ILockNamespace name, int backupCount, int asyncBackupCount) {
        this.namespace = name;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
    }

    public boolean lock(Data key, String caller, int threadId) {
        return lock(key, caller, threadId, Long.MAX_VALUE);
    }

    public boolean lock(Data key, String caller, int threadId, long ttl) {
        final LockInfo lock = getLock(key);
        return lock.lock(caller, threadId, ttl);
    }

    public boolean extendTTL(Data key, String caller, int threadId, long ttl) {
        final LockInfo lock = locks.get(key);
        return lock != null && lock.extendTTL(caller, threadId, ttl);
    }

    private LockInfo getLock(Data key) {
        return ConcurrencyUtil.getOrPutIfAbsent(locks, key, lockConstructor);
    }

    public boolean isLocked(Data key) {
        final LockInfo lock = locks.get(key);
        return lock != null && lock.isLocked();
    }

    public boolean isLockedBy(Data key, String caller, int threadId) {
        LockInfo lock = locks.get(key);
        return lock != null && lock.isLockedBy(caller, threadId);
    }

    public boolean canAcquireLock(Data key, String caller, int threadId) {
        final LockInfo lock = locks.get(key);
        return lock == null || lock.canAcquireLock(caller, threadId);
    }

    public boolean unlock(Data key, String caller, int threadId) {
        final LockInfo lock = locks.get(key);
        boolean result = false;
        if (lock == null)
            return result;
        if (lock.canAcquireLock(caller, threadId)) {
            if (lock.unlock(caller, threadId)) {
                result = true;
            }
        }
        if (lock.isEvictable()) {
            locks.remove(key);
        }
        return result;
    }

    public boolean forceUnlock(Data key) {
        final LockInfo lock = locks.get(key);
        if (lock == null)
            return false;
        else {
            if (lock.isEvictable()) {
                locks.remove(key);
            } else {
                lock.clear();
            }
            return true;
        }
    }

    public Map<Data, LockInfo> getLocks() {
        return Collections.unmodifiableMap(locks);
    }

    public Set<Data> getLockedKeys() {
        return Collections.unmodifiableSet(locks.keySet());
    }

    public void clear() {
        locks.clear();
    }

    public ILockNamespace getNamespace() {
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

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(namespace);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        int len = locks.size();
        out.writeInt(len);
        if (len > 0) {
            for (LockInfo lock : locks.values()) {
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
                LockInfo lock = new LockInfo();
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
