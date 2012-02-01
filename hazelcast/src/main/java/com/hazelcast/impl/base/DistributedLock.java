/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.base;

import com.hazelcast.nio.Address;

import java.io.Serializable;

public class DistributedLock implements Serializable {
    Address lockAddress = null;
    int lockThreadId = -1;
    int lockCount;

    public DistributedLock() {
    }

    public DistributedLock(DistributedLock copy) {
        this.lockAddress = copy.lockAddress;
        this.lockCount = copy.lockCount;
        this.lockThreadId = copy.lockThreadId;
    }

    public DistributedLock(Address address, int threadId) {
        this.lockAddress = address;
        this.lockThreadId = threadId;
        this.lockCount = 1;
    }

    public DistributedLock(Address lockAddress, int lockThreadId, int lockCount) {
        this.lockAddress = lockAddress;
        this.lockThreadId = lockThreadId;
        this.lockCount = lockCount;
    }

    public boolean isLocked() {
        return lockCount > 0;
    }

    public boolean isLockedBy(Address address, int threadId) {
        return (this.lockThreadId == threadId && this.lockAddress.equals(address));
    }

    public boolean lock(Address address, int threadId) {
        if (this.lockCount == 0) {
            this.lockAddress = address;
            this.lockThreadId = threadId;
            this.lockCount = 1;
            return true;
        } else if (isLockedBy(address, threadId)) {
            this.lockCount = 1;
            return true;
        }
        return false;
    }

    public boolean unlock(Address address, int threadId) {
        if (lockCount == 0) {
            return false;
        } else {
            if (isLockedBy(address, threadId)) {
                this.lockCount--;
                if (lockCount == 0) {
                    this.lockAddress = null;
                    this.lockThreadId = -1;
                }
                return true;
            }
        }
        return false;
    }

    public boolean testLock(int threadId, Address address) {
        return lockCount == 0 || getLockThreadId() == threadId && getLockAddress().equals(address);
    }

    public void clear() {
        lockThreadId = -1;
        lockCount = 0;
        lockAddress = null;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedLock that = (DistributedLock) o;
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
}
