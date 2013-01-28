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

package com.hazelcast.lock;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;


public class LockInfo implements DataSerializable {
    private Address lockAddress = null;
    private int lockThreadId = -1;
    private int lockCount;
    private long expirationTime = -1;
    private long lockAcquireTime = -1L;

    public LockInfo() {
    }

    public LockInfo(LockInfo copy) {
        this(copy.lockAddress, copy.lockThreadId, copy.lockCount, copy.lockAcquireTime);
    }

    public LockInfo(Address address, int threadId) {
        this(address, threadId, 1);
    }

    public LockInfo(Address lockAddress, int lockThreadId, int lockCount) {
        this(lockAddress, lockThreadId, lockCount, Clock.currentTimeMillis());
    }

    private LockInfo(final Address lockAddress, final int lockThreadId,
                     final int lockCount, final long lockAcquireTime) {
        this.lockAcquireTime = lockAcquireTime;
        this.lockAddress = lockAddress;
        this.lockCount = lockCount;
        this.lockThreadId = lockThreadId;
    }

    public boolean isLocked() {
        checkTTL();
        return lockCount > 0;
    }

    public boolean isLockedBy(Address address, int threadId) {
        checkTTL();
        return (lockThreadId == threadId && address != null && address.equals(lockAddress));
    }

    void checkTTL() {
        if (lockCount > 0 && System.currentTimeMillis() >= expirationTime) {
            clear();
        }
    }

    // todo when a member dies release the lock
    public boolean lock(Address address, int threadId, long ttl) {
        checkTTL();
        if (lockCount == 0) {
            lockAddress = address;
            lockThreadId = threadId;
            lockCount++;
            lockAcquireTime = Clock.currentTimeMillis();
            expirationTime = System.currentTimeMillis() + ttl;
            return true;
        } else if (isLockedBy(address, threadId)) {
            lockCount++;
            expirationTime = System.currentTimeMillis() + ttl;
            return true;
        }
        return false;
    }

    public boolean unlock(Address address, int threadId) {
        checkTTL();
        if (lockCount == 0) {
            return false;
        } else {
            if (isLockedBy(address, threadId)) {
                lockCount--;
                if (lockCount == 0) {
                    clear();
                }
                return true;
            }
        }
        return false;
    }

    public boolean canAcquireLock(Address address, int threadId) {
        checkTTL();
        return lockCount == 0 || getLockThreadId() == threadId && getLockAddress().equals(address);
    }

    public void clear() {
        lockThreadId = -1;
        lockCount = 0;
        lockAddress = null;
        expirationTime = 0;
        lockAcquireTime = -1L;
    }

    public Address getLockAddress() {
        return lockAddress;
    }

    public int getLockThreadId() {
        return lockThreadId;
    }

    public int getLockCount() {
        return lockCount;
    }

    public long getAcquireTime() {
        return lockAcquireTime;
    }

    public long getRemainingTTL() {
        long now = System.currentTimeMillis();
        if (now >= expirationTime) return 0;
        return expirationTime - now;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockInfo that = (LockInfo) o;
        if (lockCount != that.lockCount) return false;
        if (lockThreadId != that.lockThreadId) return false;
        if (lockAddress != null ? !lockAddress.equals(that.lockAddress) : that.lockAddress != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = lockAddress != null ? lockAddress.hashCode() : 0;
        result = 31 * result + lockThreadId;
        result = 31 * result + lockCount;
        return result;
    }

    @Override
    public String toString() {
        return "Lock{" +
                "lockAddress=" + lockAddress +
                ", lockThreadId=" + lockThreadId +
                ", lockCount=" + lockCount +
                '}';
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        if (lockAddress == null){
            out.writeBoolean(false);
        }
        else {
            out.writeBoolean(true);
            lockAddress.writeData(out);
        }

        out.writeInt(lockThreadId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(lockAcquireTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        if(in.readBoolean()){
            lockAddress = new Address();
            lockAddress.readData(in);
        }
        lockThreadId = in.readInt();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        lockAcquireTime = in.readLong();
    }
}
