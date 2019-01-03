/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.concurrent.lock.LockDataSerializerHook.F_ID;
import static com.hazelcast.concurrent.lock.LockDataSerializerHook.LOCK_RESOURCE;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.SetUtil.createHashSet;

final class LockResourceImpl implements IdentifiedDataSerializable, LockResource {

    private Data key;
    private String owner;
    private long threadId;
    private long referenceId;
    private int lockCount;
    private long expirationTime = -1;
    private long acquireTime = -1L;
    private boolean transactional;
    private boolean blockReads;
    private boolean local;
    private Map<String, WaitersInfo> waiters;
    private Set<ConditionKey> conditionKeys;
    private List<AwaitOperation> expiredAwaitOps;
    private LockStoreImpl lockStore;

    // version is stored locally
    // and incremented by 1 for each lock and extendLease operation
    private transient int version;

    public LockResourceImpl() {
    }

    public LockResourceImpl(Data key, LockStoreImpl lockStore) {
        this.key = key;
        this.lockStore = lockStore;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public boolean isLocked() {
        return lockCount > 0;
    }

    @Override
    public boolean isLockedBy(String owner, long threadId) {
        return (this.threadId == threadId && owner != null && owner.equals(this.owner));
    }

    boolean lock(String owner, long threadId, long referenceId, long leaseTime, boolean transactional,
                 boolean blockReads, boolean local) {
        if (lockCount == 0) {
            this.owner = owner;
            this.threadId = threadId;
            this.referenceId = referenceId;
            lockCount = 1;
            acquireTime = Clock.currentTimeMillis();
            setExpirationTime(leaseTime);
            this.transactional = transactional;
            this.blockReads = blockReads;
            this.local = local;
            return true;
        } else if (isLockedBy(owner, threadId)) {
            if (!transactional && !local && this.referenceId == referenceId) {
                return true;
            }
            this.referenceId = referenceId;
            lockCount++;
            setExpirationTime(leaseTime);
            this.transactional = transactional;
            this.blockReads = blockReads;
            this.local = local;
            return true;
        }
        return false;
    }

    /**
     * This method is used to extend the already locked resource in the prepare phase of the transactions.
     * It also marks the resource true to block reads.
     *
     * @param caller
     * @param threadId
     * @param leaseTime
     * @return
     */
    boolean extendLeaseTime(String caller, long threadId, long leaseTime) {
        if (!isLockedBy(caller, threadId)) {
            return false;
        }
        this.blockReads = true;
        if (expirationTime < Long.MAX_VALUE) {
            setExpirationTime(expirationTime - Clock.currentTimeMillis() + leaseTime);
        }
        return true;
    }

    private void setExpirationTime(long leaseTime) {
        version++;
        if (leaseTime < 0) {
            expirationTime = Long.MAX_VALUE;
            lockStore.cancelEviction(key);
        } else {
            expirationTime = Clock.currentTimeMillis() + leaseTime;
            if (expirationTime < 0) {
                expirationTime = Long.MAX_VALUE;
                lockStore.cancelEviction(key);
            } else {
                lockStore.scheduleEviction(key, version, leaseTime);
            }
        }
    }

    boolean unlock(String owner, long threadId, long referenceId) {
        if (lockCount == 0) {
            return false;
        }

        if (!isLockedBy(owner, threadId)) {
            return false;
        }

        if (!this.transactional && !this.local && this.referenceId == referenceId) {
            return true;
        }

        this.referenceId = referenceId;
        lockCount--;
        if (lockCount == 0) {
            clear();
        }
        return true;
    }

    boolean canAcquireLock(String caller, long threadId) {
        return lockCount == 0 || getThreadId() == threadId && getOwner().equals(caller);
    }

    void addAwait(String conditionId, String caller, long threadId) {
        if (waiters == null) {
            waiters = createHashMap(2);
        }

        WaitersInfo condition = waiters.get(conditionId);
        if (condition == null) {
            condition = new WaitersInfo(conditionId);
            waiters.put(conditionId, condition);
        }
        condition.addWaiter(caller, threadId);
    }

    void removeAwait(String conditionId, String caller, long threadId) {
        if (waiters == null) {
            return;
        }

        WaitersInfo condition = waiters.get(conditionId);
        if (condition == null) {
            return;
        }

        condition.removeWaiter(caller, threadId);
        if (!condition.hasWaiter()) {
            waiters.remove(conditionId);
        }
    }

    /**
     * Signal a waiter.
     * <p>
     * We need to pass objectName because the name in {#objectName} is unrealible.
     *
     * @param conditionId
     * @param maxSignalCount
     * @param objectName
     * @see InternalLockNamespace
     */
    public void signal(String conditionId, int maxSignalCount, String objectName) {
        if (waiters == null) {
            return;
        }

        Set<WaitersInfo.ConditionWaiter> waiters;
        WaitersInfo condition = this.waiters.get(conditionId);
        if (condition == null) {
            return;
        } else {
            waiters = condition.getWaiters();
        }
        if (waiters == null) {
            return;
        }
        Iterator<WaitersInfo.ConditionWaiter> iterator = waiters.iterator();
        for (int i = 0; iterator.hasNext() && i < maxSignalCount; i++) {
            WaitersInfo.ConditionWaiter waiter = iterator.next();
            ConditionKey signalKey = new ConditionKey(objectName,
                    key, conditionId, waiter.getCaller(), waiter.getThreadId());
            registerSignalKey(signalKey);
            iterator.remove();
        }
        if (!condition.hasWaiter()) {
            this.waiters.remove(conditionId);
        }

    }

    private void registerSignalKey(ConditionKey conditionKey) {
        if (conditionKeys == null) {
            conditionKeys = new HashSet<ConditionKey>();
        }
        conditionKeys.add(conditionKey);
    }

    ConditionKey getSignalKey() {
        Set<ConditionKey> keys = conditionKeys;
        if (isNullOrEmpty(keys)) {
            return null;
        }

        return keys.iterator().next();
    }

    void removeSignalKey(ConditionKey conditionKey) {
        if (conditionKeys != null) {
            conditionKeys.remove(conditionKey);
        }
    }

    boolean hasSignalKey(ConditionKey conditionKey) {
        if (conditionKeys == null) {
            return false;
        }
        return conditionKeys.contains(conditionKey);
    }

    void registerExpiredAwaitOp(AwaitOperation awaitResponse) {
        if (expiredAwaitOps == null) {
            expiredAwaitOps = new LinkedList<AwaitOperation>();
        }
        expiredAwaitOps.add(awaitResponse);
    }

    AwaitOperation pollExpiredAwaitOp() {
        List<AwaitOperation> ops = expiredAwaitOps;
        if (isNullOrEmpty(ops)) {
            return null;
        }

        Iterator<AwaitOperation> iterator = ops.iterator();
        AwaitOperation awaitResponse = iterator.next();
        iterator.remove();
        return awaitResponse;
    }

    void clear() {
        threadId = 0;
        lockCount = 0;
        owner = null;
        referenceId = 0L;
        expirationTime = 0;
        acquireTime = -1L;
        cancelEviction();
        version = 0;
        transactional = false;
        blockReads = false;
        local = false;
    }

    void cancelEviction() {
        lockStore.cancelEviction(key);
    }

    boolean isRemovable() {
        return !isLocked()
                && isNullOrEmpty(waiters)
                && isNullOrEmpty(expiredAwaitOps)
                && isNullOrEmpty(conditionKeys);
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public boolean isTransactional() {
        return transactional;
    }

    /**
     * Local locks are local to the partition and replicaIndex where they have been acquired.
     * That is the reason they are removed on any partition migration on the destination.
     *
     * @returns true if the lock is local, false otherwise
     */
    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public boolean shouldBlockReads() {
        return blockReads;
    }

    @Override
    public long getThreadId() {
        return threadId;
    }

    @Override
    public int getLockCount() {
        return lockCount;
    }

    @Override
    public long getAcquireTime() {
        return acquireTime;
    }

    @Override
    public long getRemainingLeaseTime() {
        if (!isLocked()) {
            return -1L;
        }
        if (expirationTime < 0) {
            return Long.MAX_VALUE;
        }
        long now = Clock.currentTimeMillis();
        if (now >= expirationTime) {
            return 0;
        }
        return expirationTime - now;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public int getVersion() {
        return version;
    }

    void setLockStore(LockStoreImpl lockStore) {
        this.lockStore = lockStore;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return LOCK_RESOURCE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData(key);
        out.writeUTF(owner);
        out.writeLong(threadId);
        out.writeLong(referenceId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(acquireTime);
        out.writeBoolean(transactional);
        out.writeBoolean(blockReads);

        int conditionCount = getConditionCount();
        out.writeInt(conditionCount);
        if (conditionCount > 0) {
            for (WaitersInfo condition : waiters.values()) {
                condition.writeData(out);
            }
        }
        int signalCount = getSignalCount();
        out.writeInt(signalCount);
        if (signalCount > 0) {
            for (ConditionKey signalKey : conditionKeys) {
                out.writeUTF(signalKey.getObjectName());
                out.writeUTF(signalKey.getConditionId());
                out.writeUTF(signalKey.getUuid());
                out.writeLong(signalKey.getThreadId());
            }
        }
        int expiredAwaitOpsCount = getExpiredAwaitsOpsCount();
        out.writeInt(expiredAwaitOpsCount);
        if (expiredAwaitOpsCount > 0) {
            for (AwaitOperation op : expiredAwaitOps) {
                op.writeData(out);
            }
        }
    }

    private int getExpiredAwaitsOpsCount() {
        return expiredAwaitOps == null ? 0 : expiredAwaitOps.size();
    }

    private int getSignalCount() {
        return conditionKeys == null ? 0 : conditionKeys.size();
    }

    private int getConditionCount() {
        return waiters == null ? 0 : waiters.size();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readData();
        owner = in.readUTF();
        threadId = in.readLong();
        referenceId = in.readLong();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        acquireTime = in.readLong();
        transactional = in.readBoolean();
        blockReads = in.readBoolean();

        int len = in.readInt();
        if (len > 0) {
            waiters = createHashMap(len);
            for (int i = 0; i < len; i++) {
                WaitersInfo condition = new WaitersInfo();
                condition.readData(in);
                waiters.put(condition.getConditionId(), condition);
            }
        }

        len = in.readInt();
        if (len > 0) {
            conditionKeys = createHashSet(len);
            for (int i = 0; i < len; i++) {
                conditionKeys.add(new ConditionKey(in.readUTF(), key, in.readUTF(), in.readUTF(), in.readLong()));
            }
        }

        len = in.readInt();
        if (len > 0) {
            expiredAwaitOps = new ArrayList<AwaitOperation>(len);
            for (int i = 0; i < len; i++) {
                AwaitOperation op = new AwaitOperation();
                op.readData(in);
                expiredAwaitOps.add(op);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockResourceImpl that = (LockResourceImpl) o;
        if (threadId != that.threadId) {
            return false;
        }
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LockResource{"
                + "owner='" + owner + '\''
                + ", threadId=" + threadId
                + ", lockCount=" + lockCount
                + ", acquireTime=" + acquireTime
                + ", expirationTime=" + expirationTime
                + '}';
    }

    private static boolean isNullOrEmpty(Collection c) {
        return c == null || c.isEmpty();
    }

    private static boolean isNullOrEmpty(Map m) {
        return m == null || m.isEmpty();
    }

    void cleanWaitersAndSignalsFor(String uuid) {
        if (conditionKeys != null) {
            Iterator<ConditionKey> iter = conditionKeys.iterator();
            while (iter.hasNext()) {
                ConditionKey conditionKey = iter.next();
                if (conditionKey.getUuid().equals(uuid)) {
                    iter.remove();
                }
            }
        }
        if (waiters != null) {
            for (WaitersInfo waitersInfo : waiters.values()) {
                Iterator<WaitersInfo.ConditionWaiter> iter = waitersInfo.getWaiters().iterator();
                while (iter.hasNext()) {
                    WaitersInfo.ConditionWaiter waiter = iter.next();
                    if (waiter.getCaller().equals(uuid)) {
                        iter.remove();
                    }
                }
            }
        }
    }
}
