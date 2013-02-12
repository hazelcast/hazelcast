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

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;


public class LockInfo implements DataSerializable {
    private String lockOwner = null;
    private int lockThreadId = -1;
    private int lockCount;
    private long expirationTime = -1;
    private long lockAcquireTime = -1L;

    public LockInfo() {
    }

    public LockInfo(LockInfo copy) {
        this(copy.lockOwner, copy.lockThreadId, copy.lockCount, copy.lockAcquireTime);
    }

    public LockInfo(String owner, int threadId) {
        this(owner, threadId, 1);
    }

    public LockInfo(String lockOwner, int lockThreadId, int lockCount) {
        this(lockOwner, lockThreadId, lockCount, Clock.currentTimeMillis());
    }

    private LockInfo(final String lockOwner, final int lockThreadId,
                     final int lockCount, final long lockAcquireTime) {
        this.lockAcquireTime = lockAcquireTime;
        this.lockOwner = lockOwner;
        this.lockCount = lockCount;
        this.lockThreadId = lockThreadId;
    }

    public boolean isLocked() {
        checkTTL();
        return lockCount > 0;
    }

    public boolean isLockedBy(String owner, int threadId) {
        checkTTL();
        return (lockThreadId == threadId && owner != null && owner.equals(lockOwner));
    }

    void checkTTL() {
        if (lockCount > 0 && System.currentTimeMillis() >= expirationTime) {
            clear();
        }
    }

    public boolean lock(String owner, int threadId, long ttl) {
        checkTTL();
        if (lockCount == 0) {
            lockOwner = owner;
            lockThreadId = threadId;
            lockCount++;
            lockAcquireTime = Clock.currentTimeMillis();
            expirationTime = System.currentTimeMillis() + ttl;
            return true;
        } else if (isLockedBy(owner, threadId)) {
            lockCount++;
            expirationTime = System.currentTimeMillis() + ttl;
            return true;
        }
        return false;
    }

    public boolean unlock(String owner, int threadId) {
        checkTTL();
        if (lockCount == 0) {
            return false;
        } else {
            if (isLockedBy(owner, threadId)) {
                lockCount--;
                if (lockCount == 0) {
                    clear();
                }
                return true;
            }
        }
        return false;
    }

    public boolean canAcquireLock(String owner, int threadId) {
        checkTTL();
        return lockCount == 0 || getLockThreadId() == threadId && getLockOwner().equals(owner);
    }

    public void clear() {
        lockThreadId = -1;
        lockCount = 0;
        lockOwner = null;
        expirationTime = 0;
        lockAcquireTime = -1L;
    }

    public String getLockOwner() {
        return lockOwner;
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
        if (lockOwner != null ? !lockOwner.equals(that.lockOwner) : that.lockOwner != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = lockOwner != null ? lockOwner.hashCode() : 0;
        result = 31 * result + lockThreadId;
        result = 31 * result + lockCount;
        return result;
    }

    @Override
    public String toString() {
        return "Lock{" +
                "lockString=" + lockOwner +
                ", lockThreadId=" + lockThreadId +
                ", lockCount=" + lockCount +
                '}';
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeNullableString(out, lockOwner);
        out.writeInt(lockThreadId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(lockAcquireTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        lockOwner = IOUtil.readNullableString(in);
        lockThreadId = in.readInt();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        lockAcquireTime = in.readLong();
    }
}
