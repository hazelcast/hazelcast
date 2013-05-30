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
import com.hazelcast.nio.serialization.*;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.*;

public class DistributedLock implements DataSerializable {

    private Data key;
    private String owner = null;
    private int threadId = -1;
    private int lockCount;
    private long expirationTime = -1;
    private long acquireTime = -1L;
    private boolean transactional = false;

    private Map<String, ConditionInfo> conditions;
    private List<ConditionKey> signalKeys;
    private List<AwaitOperation> expiredAwaitOps;
    private transient LockService lockService;
    private transient ObjectNamespace namespace;

    public DistributedLock() {
    }

    public DistributedLock(Data key, LockService lockService, ObjectNamespace namespace) {
        this.key = key;
        this.lockService = lockService;
        this.namespace = namespace;
    }

    public Data getKey() {
        return key;
    }

    public void setLockService(LockService lockService) {
        this.lockService = lockService;
    }

    public void setNamespace(ObjectNamespace namespace) {
        this.namespace = namespace;
    }

    public boolean isLocked() {
        return lockCount > 0;
    }

    public boolean isLockedBy(String owner, int threadId) {
        return (this.threadId == threadId && owner != null && owner.equals(this.owner));
    }

    public boolean lock(String owner, int threadId, long ttl) {
        return lock(owner, threadId, ttl, false);
    }

    public boolean lock(String owner, int threadId, long ttl, boolean transactional) {
        if (lockCount == 0) {
            this.owner = owner;
            this.threadId = threadId;
            lockCount++;
            acquireTime = Clock.currentTimeMillis();
            setExpirationTime(ttl);
            this.transactional = transactional;
            return true;
        } else if (isLockedBy(owner, threadId)) {
            lockCount++;
            setExpirationTime(ttl);
            this.transactional = transactional;
            return true;
        }
        this.transactional = false;
        return false;
    }

    public boolean extendTTL(String caller, int threadId, long ttl) {
        if (isLockedBy(caller, threadId)) {
            if (expirationTime < Long.MAX_VALUE) {
                setExpirationTime(expirationTime - Clock.currentTimeMillis() + ttl);
                lockService.scheduleEviction(namespace, key, ttl);
            }
            return true;
        }
        return false;
    }

    private void setExpirationTime(long ttl) {
        if (ttl < 0) {
            expirationTime = Long.MAX_VALUE;
        } else {
            expirationTime = Clock.currentTimeMillis() + ttl;
            if (expirationTime < 0) {
                expirationTime = Long.MAX_VALUE;
            } else {
                lockService.scheduleEviction(namespace, key, ttl);
            }
        }
    }

    public boolean unlock(String owner, int threadId) {
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

    public boolean canAcquireLock(String caller, int threadId) {
        return lockCount == 0 || getThreadId() == threadId && getOwner().equals(caller);
    }

    boolean addAwait(String conditionId, String caller, int threadId) {
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

    boolean removeAwait(String conditionId, String caller, int threadId) {
        if (conditions != null) {
            final ConditionInfo condition = conditions.get(conditionId);
            if (condition != null) {
                final boolean ok = condition.removeWaiter(caller, threadId);
                if (condition.getAwaitCount() == 0) {
                    conditions.remove(conditionId);
                }
                return ok;
            }
        }
        return false;
    }

    boolean startAwaiting(String conditionId, String caller, int threadId) {
        if (conditions != null) {
            final ConditionInfo condition = conditions.get(conditionId);
            if (condition != null) {
                return condition.startWaiter(caller, threadId);
            }
        }
        return false;
    }

    int getAwaitCount(String conditionId) {
        if (conditions != null) {
            final ConditionInfo condition = conditions.get(conditionId);
            return condition != null ? condition.getAwaitCount() : 0;
        }
        return 0;
    }

    void registerSignalKey(ConditionKey conditionKey) {
        if (signalKeys == null) {
            signalKeys = new LinkedList<ConditionKey>();
        }
        signalKeys.add(conditionKey);
    }

    ConditionKey getSignalKey() {
        final List<ConditionKey> keys = signalKeys;
        return keys != null && !keys.isEmpty() ? keys.iterator().next() : null;
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
        final List<AwaitOperation> ops = expiredAwaitOps;
        if (ops != null && !ops.isEmpty()) {
            Iterator<AwaitOperation> iter = ops.iterator();
            AwaitOperation awaitResponse = iter.next();
            iter.remove();
            return awaitResponse;
        }
        return null;
    }

    public void clear() {
        threadId = -1;
        lockCount = 0;
        owner = null;
        expirationTime = 0;
        acquireTime = -1L;
        cancelEviction();
    }

    public void cancelEviction() {
        lockService.cancelEviction(namespace, key);
    }

    public boolean isRemovable() {
        return !isLocked()
                && (conditions == null || conditions.isEmpty())
                && (expiredAwaitOps == null || expiredAwaitOps.isEmpty());
    }

    public String getOwner() {
        return owner;
    }

    public boolean isTransactional() {
        return transactional;
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
        long now = Clock.currentTimeMillis();
        if (now >= expirationTime) return 0;
        return expirationTime - now;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedLock that = (DistributedLock) o;
        if (threadId != that.threadId) return false;
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + threadId;
        return result;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        key.writeData(out);
        out.writeUTF(owner);
        out.writeInt(threadId);
        out.writeInt(lockCount);
        out.writeLong(expirationTime);
        out.writeLong(acquireTime);
        out.writeBoolean(transactional);

        int len = conditions == null ? 0 : conditions.size();
        out.writeInt(len);
        if (len > 0) {
            for (ConditionInfo condition : conditions.values()) {
                condition.writeData(out);
            }
        }
        len = signalKeys == null ? 0 : signalKeys.size();
        out.writeInt(len);
        if (len > 0) {
            for (ConditionKey key : signalKeys) {
                out.writeUTF(key.getConditionId());
            }
        }
        len = expiredAwaitOps == null ? 0 : expiredAwaitOps.size();
        out.writeInt(len);
        if (len > 0) {
            for (AwaitOperation op : expiredAwaitOps) {
                op.writeData(out);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        key = new Data();
        key.readData(in);
        owner = in.readUTF();
        threadId = in.readInt();
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
                signalKeys.add(new ConditionKey(key, in.readUTF()));
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
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DistributedLock");
        sb.append("{owner='").append(owner).append('\'');
        sb.append(", threadId=").append(threadId);
        sb.append(", lockCount=").append(lockCount);
        sb.append(", acquireTime=").append(acquireTime);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append('}');
        return sb.toString();
    }
}
