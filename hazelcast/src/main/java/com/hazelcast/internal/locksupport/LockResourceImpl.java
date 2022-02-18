/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import static com.hazelcast.internal.locksupport.LockDataSerializerHook.F_ID;
import static com.hazelcast.internal.locksupport.LockDataSerializerHook.LOCK_RESOURCE;

final class LockResourceImpl implements IdentifiedDataSerializable, LockResource {

    private Data key;
    private UUID owner;
    private long threadId;
    private long referenceId;
    private int lockCount;
    private long expirationTime = -1;
    private long acquireTime = -1L;
    private boolean transactional;
    private boolean blockReads;
    private boolean local;
    private LockStoreImpl lockStore;

    // version is stored locally
    // and incremented by 1 for each lock and extendLease operation
    private transient int version;

    LockResourceImpl() {
    }

    LockResourceImpl(Data key, LockStoreImpl lockStore) {
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
    public boolean isLockedBy(UUID owner, long threadId) {
        return (this.threadId == threadId && owner != null && owner.equals(this.owner));
    }

    boolean lock(UUID owner, long threadId, long referenceId, long leaseTime, boolean transactional,
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
    boolean extendLeaseTime(UUID caller, long threadId, long leaseTime) {
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

    boolean unlock(UUID owner, long threadId, long referenceId) {
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

    boolean canAcquireLock(UUID caller, long threadId) {
        return lockCount == 0 || getThreadId() == threadId && getOwner().equals(caller);
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
        return !isLocked();
    }

    @Override
    public UUID getOwner() {
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
    public int getClassId() {
        return LOCK_RESOURCE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, key);
        UUIDSerializationUtil.writeUUID(out, owner);
        out.writeLong(threadId);
        out.writeLong(referenceId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(acquireTime);
        out.writeBoolean(transactional);
        out.writeBoolean(blockReads);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readData(in);
        owner = UUIDSerializationUtil.readUUID(in);
        threadId = in.readLong();
        referenceId = in.readLong();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        acquireTime = in.readLong();
        transactional = in.readBoolean();
        blockReads = in.readBoolean();
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
        return Objects.equals(owner, that.owner);
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
}
