/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.scheduler.EntryTaskScheduler;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.concurrent.lock.LockDataSerializerHook.F_ID;
import static com.hazelcast.concurrent.lock.LockDataSerializerHook.LOCK_STORE;
import static com.hazelcast.concurrent.lock.ObjectNamespaceSerializationHelper.readNamespaceCompatibly;
import static com.hazelcast.concurrent.lock.ObjectNamespaceSerializationHelper.writeNamespaceCompatibly;

public final class LockStoreImpl implements IdentifiedDataSerializable, LockStore, Versioned {

    private final transient ConstructorFunction<Data, LockResourceImpl> lockConstructor =
            new ConstructorFunction<Data, LockResourceImpl>() {
                public LockResourceImpl createNew(Data key) {
                    return new LockResourceImpl(key, LockStoreImpl.this);
                }
            };

    private final ConcurrentMap<Data, LockResourceImpl> locks = new ConcurrentHashMap<Data, LockResourceImpl>();

    // warning: the namespace field is unreliable if this LockStoreImpl was created for ILock proxy
    // ObjectNameSpace.getObjectName() can give you a wrong name because LockStoreImpl instances
    // are shared for ILock proxies. see InternalLockNamespace for details.
    private ObjectNamespace namespace;
    private int backupCount;
    private int asyncBackupCount;

    private LockService lockService;
    private EntryTaskScheduler<Data, Integer> entryTaskScheduler;

    public LockStoreImpl() {
    }

    public LockStoreImpl(LockService lockService, ObjectNamespace name,
                         EntryTaskScheduler<Data, Integer> entryTaskScheduler, int backupCount, int asyncBackupCount) {
        this.lockService = lockService;
        this.namespace = name;
        this.entryTaskScheduler = entryTaskScheduler;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
    }

    @Override
    public boolean lock(Data key, String caller, long threadId, long referenceId, long leaseTime) {
        leaseTime = getLeaseTime(leaseTime);
        LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, referenceId, leaseTime, false, false, false);
    }

    @Override
    public boolean localLock(Data key, String caller, long threadId, long referenceId, long leaseTime) {
        // local locks can observe max lease time since they are used internally for EntryProcessor write Offloading
        LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, referenceId, leaseTime, false, false, true);
    }

    private long getLeaseTime(long leaseTime) {
        long maxLeaseTimeInMillis = lockService.getMaxLeaseTimeInMillis();
        if (leaseTime > maxLeaseTimeInMillis) {
            throw new IllegalArgumentException("Max allowed lease time: " + maxLeaseTimeInMillis + "ms. "
                    + "Given lease time: " + leaseTime + "ms.");
        }
        if (leaseTime < 0) {
            leaseTime = maxLeaseTimeInMillis;
        }
        return leaseTime;
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long referenceId, long leaseTime, boolean blockReads) {
        LockResourceImpl lock = getLock(key);
        return lock.lock(caller, threadId, referenceId, leaseTime, true, blockReads, false);
    }

    @Override
    public boolean extendLeaseTime(Data key, String caller, long threadId, long leaseTime) {
        LockResourceImpl lock = locks.get(key);
        return lock != null && lock.extendLeaseTime(caller, threadId, leaseTime);
    }

    public LockResourceImpl getLock(Data key) {
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
        return lock != null && lock.isLockedBy(caller, threadId);
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
    public int getLockedEntryCount() {
        return locks.size();
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
        return lock == null || lock.canAcquireLock(caller, threadId);
    }

    @Override
    public boolean shouldBlockReads(Data key) {
        LockResourceImpl lock = locks.get(key);
        return lock != null && lock.shouldBlockReads() && lock.isLocked();
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId, long referenceId) {
        LockResourceImpl lock = locks.get(key);
        if (lock == null) {
            return false;
        }

        boolean result = false;
        if (lock.canAcquireLock(caller, threadId)) {
            if (lock.unlock(caller, threadId, referenceId)) {
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

    void cleanWaitersAndSignalsFor(Data key, String uuid) {
        LockResourceImpl lockResource = locks.get(key);
        if (lockResource != null) {
            lockResource.cleanWaitersAndSignalsFor(uuid);
        }
    }

    public int getVersion(Data key) {
        LockResourceImpl lock = locks.get(key);
        if (lock != null) {
            return lock.getVersion();
        }
        return -1;
    }

    public Collection<LockResource> getLocks() {
        return Collections.<LockResource>unmodifiableCollection(locks.values());
    }

    public void removeLocalLocks() {
        for (Iterator<Map.Entry<Data, LockResourceImpl>> iterator = locks.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<Data, LockResourceImpl> entry = iterator.next();
            if (entry.getValue().isLocal()) {
                iterator.remove();
            }
        }
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

    public boolean hasLock() {
        return !locks.isEmpty();
    }

    void scheduleEviction(Data key, int version, long leaseTime) {
        entryTaskScheduler.schedule(leaseTime, key, version);
    }

    void cancelEviction(Data key) {
        entryTaskScheduler.cancel(key);
    }

    void setLockService(LockServiceImpl lockService) {
        this.lockService = lockService;
    }

    void setEntryTaskScheduler(EntryTaskScheduler<Data, Integer> entryTaskScheduler) {
        this.entryTaskScheduler = entryTaskScheduler;
    }

    public void clear() {
        locks.clear();
        entryTaskScheduler.cancelAll();
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

    public void addAwait(Data key, String conditionId, String caller, long threadId) {
        LockResourceImpl lock = getLock(key);
        lock.addAwait(conditionId, caller, threadId);
    }

    public void removeAwait(Data key, String conditionId, String caller, long threadId) {
        LockResourceImpl lock = getLock(key);
        lock.removeAwait(conditionId, caller, threadId);
    }

    public void signal(Data key, String conditionId, int maxSignalCount, String objectName) {
        LockResourceImpl lock = getLock(key);
        lock.signal(conditionId, maxSignalCount, objectName);
    }

    public WaitNotifyKey getNotifiedKey(Data key) {
        ConditionKey conditionKey = getSignalKey(key);
        if (conditionKey == null) {
            return new LockWaitNotifyKey(namespace, key);
        }
        return conditionKey;
    }

    private ConditionKey getSignalKey(Data key) {
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

    public boolean hasSignalKey(ConditionKey conditionKey) {
        LockResourceImpl lock = locks.get(conditionKey.getKey());
        return lock != null && lock.hasSignalKey(conditionKey);
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
            return "Owner: " + lock.getOwner() + ", thread ID: " + lock.getThreadId();
        }
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return LOCK_STORE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeNamespaceCompatibly(namespace, out);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        int len = 0;
        for (LockResourceImpl lock : locks.values()) {
            if (!lock.isLocal()) {
                len++;
            }
        }
        out.writeInt(len);
        if (len > 0) {
            for (LockResourceImpl lock : locks.values()) {
                if (!lock.isLocal()) {
                    lock.writeData(out);
                }
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        namespace = readNamespaceCompatibly(in);
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
        return "LockStoreImpl{"
                + "namespace=" + namespace
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + '}';
    }
}
