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


class LockInfo implements DataSerializable {

    private String owner = null;
    private int threadId = -1;
    private int lockCount;
    private long expirationTime = -1;
    private long acquireTime = -1L;

    public LockInfo() {
    }

    public boolean isLocked() {
        checkTTL();
        return lockCount > 0;
    }

    public boolean isLockedBy(String owner, int threadId) {
        checkTTL();
        return (this.threadId == threadId && owner != null && owner.equals(this.owner));
    }

    void checkTTL() {
        if (lockCount > 0 && System.currentTimeMillis() >= expirationTime) {
            clear();
        }
    }

    public boolean lock(String owner, int threadId, long ttl) {
        checkTTL();
        if (lockCount == 0) {
            this.owner = owner;
            this.threadId = threadId;
            lockCount++;
            acquireTime = Clock.currentTimeMillis();
            expirationTime = Clock.currentTimeMillis() + ttl;
            return true;
        } else if (isLockedBy(owner, threadId)) {
            lockCount++;
            expirationTime = Clock.currentTimeMillis() + ttl;
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
        return lockCount == 0 || getThreadId() == threadId && getOwner().equals(owner);
    }

    public void clear() {
        threadId = -1;
        lockCount = 0;
        owner = null;
        expirationTime = 0;
        acquireTime = -1L;
    }

    public String getOwner() {
        return owner;
    }

    public int getThreadId() {
        return threadId;
    }

    public int getLockCount() {
        return lockCount;
    }

    public long getAcquireTime() {
        return acquireTime;
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
        if (threadId != that.threadId) return false;
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + threadId;
        result = 31 * result + lockCount;
        return result;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeNullableString(out, owner);
        out.writeInt(threadId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(acquireTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        owner = IOUtil.readNullableString(in);
        threadId = in.readInt();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        acquireTime = in.readLong();
    }

    @Override
    public String toString() {
        return "Lock{" +
                "lockString=" + owner +
                ", lockThreadId=" + threadId +
                ", lockCount=" + lockCount +
                '}';
    }
}
