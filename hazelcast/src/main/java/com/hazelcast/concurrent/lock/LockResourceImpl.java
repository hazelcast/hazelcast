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

import com.hazelcast.concurrent.lock.operations.AwaitOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

final class LockResourceImpl implements DataSerializable, LockResource {

    private Data key;
    private String owner;
    private long threadId;
    private int lockCount;
    private long expirationTime = -1;
    private long acquireTime = -1L;
    private boolean transactional;
    private Map<String, ConditionInfo> conditions;
    private List<ConditionKey> signalKeys;
    private List<AwaitOperation> expiredAwaitOps;
    private LockStoreImpl lockStore;

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

    boolean lock(String owner, long threadId, long leaseTime) {
        return lock(owner, threadId, leaseTime, false);
    }

    boolean lock(String owner, long threadId, long leaseTime, boolean transactional) {
        if (lockCount == 0) {
            this.owner = owner;
            this.threadId = threadId;
            lockCount++;
            acquireTime = Clock.currentTimeMillis();
            setExpirationTime(leaseTime);
            this.transactional = transactional;
            return true;
        } else if (isLockedBy(owner, threadId)) {
            lockCount++;
            setExpirationTime(leaseTime);
            this.transactional = transactional;
            return true;
        }
        this.transactional = false;
        return false;
    }

    boolean extendLeaseTime(String caller, long threadId, long leaseTime) {
        if (!isLockedBy(caller, threadId)) {
            return false;
        }

        if (expirationTime < Long.MAX_VALUE) {
            setExpirationTime(expirationTime - Clock.currentTimeMillis() + leaseTime);
        }
        return true;
    }

    private void setExpirationTime(long leaseTime) {
        if (leaseTime < 0) {
            expirationTime = Long.MAX_VALUE;
            lockStore.cancelEviction(key);
        } else {
            expirationTime = Clock.currentTimeMillis() + leaseTime;
            if (expirationTime < 0) {
                expirationTime = Long.MAX_VALUE;
                lockStore.cancelEviction(key);
            } else {
                lockStore.scheduleEviction(key, leaseTime);
            }
        }
    }

    boolean unlock(String owner, long threadId) {
        if (lockCount == 0) {
            return false;
        }

        if (!isLockedBy(owner, threadId)) {
            return false;
        }

        lockCount--;
        if (lockCount == 0) {
            clear();
        }
        return true;
    }

    boolean canAcquireLock(String caller, long threadId) {
        return lockCount == 0 || getThreadId() == threadId && getOwner().equals(caller);
    }

    boolean addAwait(String conditionId, String caller, long threadId) {
        if (conditions == null) {
            conditions = new HashMap<String, ConditionInfo>(2);
        }

        ConditionInfo condition = conditions.get(conditionId);
        if (condition == null) {
            condition = new ConditionInfo(conditionId);
            conditions.put(conditionId, condition);
        }
        return condition.addWaiter(caller, threadId);
    }

    boolean removeAwait(String conditionId, String caller, long threadId) {
        if (conditions == null) {
            return false;
        }

        ConditionInfo condition = conditions.get(conditionId);
        if (condition == null) {
            return false;
        }

        boolean ok = condition.removeWaiter(caller, threadId);
        if (condition.getAwaitCount() == 0) {
            conditions.remove(conditionId);
        }
        return ok;
    }

    boolean startAwaiting(String conditionId, String caller, long threadId) {
        if (conditions == null) {
            return false;
        }

        ConditionInfo condition = conditions.get(conditionId);
        if (condition == null) {
            return false;
        }

        return condition.startWaiter(caller, threadId);
    }

    int getAwaitCount(String conditionId) {
        if (conditions == null) {
            return 0;
        }

        ConditionInfo condition = conditions.get(conditionId);
        if (condition == null) {
            return 0;
        } else {
            return condition.getAwaitCount();
        }
    }

    void registerSignalKey(ConditionKey conditionKey) {
        if (signalKeys == null) {
            signalKeys = new LinkedList<ConditionKey>();
        }
        signalKeys.add(conditionKey);
    }

    ConditionKey getSignalKey() {
        List<ConditionKey> keys = signalKeys;
        if (isNullOrEmpty(keys)) {
            return null;
        }

        return keys.iterator().next();
    }

    void removeSignalKey(ConditionKey conditionKey) {
        if (signalKeys != null) {
            signalKeys.remove(conditionKey);
        }
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
        expirationTime = 0;
        acquireTime = -1L;
        cancelEviction();
    }

    void cancelEviction() {
        lockStore.cancelEviction(key);
    }

    boolean isRemovable() {
        return !isLocked()
                && isNullOrEmpty(conditions)
                && isNullOrEmpty(expiredAwaitOps);
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public boolean isTransactional() {
        return transactional;
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
        if (expirationTime == Long.MAX_VALUE && expirationTime < 0) {
            return Long.MAX_VALUE;
        }
        long now = Clock.currentTimeMillis();
        if (now >= expirationTime) {
            return 0;
        }
        return expirationTime - now;
    }

    void setLockStore(LockStoreImpl lockStore) {
        this.lockStore = lockStore;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        key.writeData(out);
        out.writeUTF(owner);
        out.writeLong(threadId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(acquireTime);
        out.writeBoolean(transactional);

        int conditionCount = getConditionCount();
        out.writeInt(conditionCount);
        if (conditionCount > 0) {
            for (ConditionInfo condition : conditions.values()) {
                condition.writeData(out);
            }
        }
        int signalCount = getSignalCount();
        out.writeInt(signalCount);
        if (signalCount > 0) {
            for (ConditionKey signalKey : signalKeys) {
                out.writeUTF(signalKey.getObjectName());
                out.writeUTF(signalKey.getConditionId());
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
        return signalKeys == null ? 0 : signalKeys.size();
    }

    private int getConditionCount() {
        return conditions == null ? 0 : conditions.size();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = new Data();
        key.readData(in);
        owner = in.readUTF();
        threadId = in.readLong();
        lockCount = in.readInt();
        expirationTime = in.readLong();
        acquireTime = in.readLong();
        transactional = in.readBoolean();

        int len = in.readInt();
        if (len > 0) {
            conditions = new HashMap<String, ConditionInfo>(len);
            for (int i = 0; i < len; i++) {
                ConditionInfo condition = new ConditionInfo();
                condition.readData(in);
                conditions.put(condition.getConditionId(), condition);
            }
        }

        len = in.readInt();
        if (len > 0) {
            signalKeys = new ArrayList<ConditionKey>(len);
            for (int i = 0; i < len; i++) {
                signalKeys.add(new ConditionKey(in.readUTF(), key, in.readUTF()));
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
        final StringBuilder sb = new StringBuilder();
        sb.append("LockResource");
        sb.append("{owner='").append(owner).append('\'');
        sb.append(", threadId=").append(threadId);
        sb.append(", lockCount=").append(lockCount);
        sb.append(", acquireTime=").append(acquireTime);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append('}');
        return sb.toString();
    }

    private static boolean isNullOrEmpty(Collection c) {
        return c == null || c.isEmpty();
    }

    private static boolean isNullOrEmpty(Map m) {
        return m == null || m.isEmpty();
    }
}
