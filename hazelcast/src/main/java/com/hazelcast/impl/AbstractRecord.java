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

package com.hazelcast.impl;

import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.IOUtil.toObject;

@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord extends AbstractSimpleRecord implements Record {

    protected volatile int hits = 0;
    protected volatile long version = 0;
    protected volatile long maxIdleMillis = Long.MAX_VALUE;
    protected volatile long writeTime = -1;
    protected volatile long removeTime = 0;
    protected volatile long lastAccessTime = 0;
    protected volatile long lastStoredTime = 0;
    protected volatile long creationTime = 0;
    protected volatile long expirationTime = Long.MAX_VALUE;
    protected volatile long lastUpdateTime = 0;
    protected volatile boolean dirty = false;

    protected volatile DistributedLock lock = null;

    protected volatile OptionalInfo optionalInfo = null;

    public AbstractRecord(CMap cmap, int blockId, Data key, long ttl, long maxIdleMillis, long id) {
        super(blockId, cmap, id, key);
        this.setCreationTime(Clock.currentTimeMillis());
        this.setTTL(ttl);
        this.maxIdleMillis = (maxIdleMillis == 0) ? Long.MAX_VALUE : maxIdleMillis;
        this.setVersion(0);
    }

    public void runBackupOps() {
        final Set<VersionedBackupOp> backupOps = getBackupOps();
        if (backupOps != null && !backupOps.isEmpty()) {
            Iterator<VersionedBackupOp> it = backupOps.iterator();
            while (it.hasNext()) {
                VersionedBackupOp bo = it.next();
                if (bo.getVersion() < getVersion() + 1) {
                    it.remove();
                } else if (bo.getVersion() == getVersion() + 1) {
                    bo.run();
                    setVersion(bo.getVersion());
                    it.remove();
                } else {
                    return;
                }
            }
        }
    }

    public void addBackupOp(VersionedBackupOp bo) {
        if (getBackupOps() == null) {
            setBackupOps(new TreeSet<VersionedBackupOp>());
        }
        getBackupOps().add(bo);
        if (getBackupOps().size() > 4) {
            forceBackupOps();
        }
    }

    public void forceBackupOps() {
        if (getBackupOps() == null) return;
        Iterator<VersionedBackupOp> it = getBackupOps().iterator();
        while (it.hasNext()) {
            VersionedBackupOp v = it.next();
            v.run();
            setVersion(v.getVersion());
            it.remove();
        }
    }

    public Object getKey() {
        return toObject(key);
    }

    public Long[] getIndexes() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().indexes;
    }

    public byte[] getIndexTypes() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().indexTypes;
    }

    public void setIndexes(Long[] indexes, byte[] indexTypes) {
        if (indexes != null) {
            this.getOptionalInfo().indexes = indexes;
            this.getOptionalInfo().indexTypes = indexTypes;
        }
    }

    // called from ServiceThread
    public boolean lock(int threadId, Address address) {
        invalidateValueCache();
        final DistributedLock dl = lock;
        if (dl == null) {
            lock = new DistributedLock(address, threadId);
            return true;
        }
        if (dl.lock(address, threadId)) {
            lock = dl;
            return true;
        }
        return false;
    }

    // called from ServiceThread
    public boolean unlock(int threadId, Address address) {
        invalidateValueCache();
        final DistributedLock dl = lock;
        return dl == null || dl.unlock(address, threadId);
    }

    public boolean testLock(int threadId, Address address) {
        final DistributedLock dl = lock;
        return dl == null || dl.testLock(threadId, address);
    }

    public DistributedLock getLock() {
        return lock;
    }

    public void setLock(DistributedLock lock) {
        this.lock = lock;
    }

    public boolean isLocked() {
        final DistributedLock dl = lock;
        return dl != null && dl.isLocked();
    }

    public int getLockCount() {
        final DistributedLock dl = lock;
        return (dl == null) ? 0 : dl.getLockCount();
    }

    // called from ServiceThread
    public void clearLock() {
        lock = null;
    }

    public Address getLockAddress() {
        final DistributedLock dl = lock;
        return (dl == null) ? null : dl.getLockAddress();
    }

    public long getLockAcquireTime() {
        final DistributedLock dl = lock;
        return (dl != null ? dl.getAcquireTime() : -1L);
    }

    protected void invalidateValueCache() {
    }

    public void addScheduledAction(ScheduledAction scheduledAction) {
        if (getScheduledActions() == null) {
            setScheduledActions(new LinkedList<ScheduledAction>());
        }
        getScheduledActions().add(scheduledAction);
    }

    public boolean isRemovable() {
        return !isActive() && valueCount() <= 0 && getLockCount() <= 0 && !hasListener()
               && getScheduledActionCount() == 0 && getBackupOpCount() == 0;
    }

    public boolean isEvictable() {
        return getLockCount() <= 0 && !hasListener() && getScheduledActionCount() == 0;
    }

    public boolean hasListener() {
        return getListeners() != null && getListeners().size() > 0;
    }

    public void addListener(Address address, boolean returnValue) {
        if (getListeners() == null) {
            setMapListeners(new ConcurrentHashMap<Address, Boolean>(1));
        }
        getListeners().put(address, returnValue);
    }

    public void removeListener(Address address) {
        if (getListeners() == null) {
            return;
        }
        getListeners().remove(address);
    }

    public void setLastUpdated() {
        if (expirationTime != Long.MAX_VALUE && expirationTime > 0) {
            long ttl = expirationTime - (lastUpdateTime > 0L ? lastUpdateTime : creationTime);
            setTTL(ttl);
        }
        setLastUpdateTime(Clock.currentTimeMillis());
    }

    public void setLastAccessed() {
        setLastAccessTime(Clock.currentTimeMillis());
        incrementHits();
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public long getRemainingTTL() {
        if (expirationTime == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        } else {
            long ttl = expirationTime - Clock.currentTimeMillis();
            return (ttl < 0) ? 1 : ttl;
        }
    }

    public long getRemainingIdle() {
        if (maxIdleMillis == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        } else {
            long lastTouch = Math.max(lastAccessTime, creationTime);
            long idle = Clock.currentTimeMillis() - lastTouch;
            return maxIdleMillis - idle;
        }
    }

    public void setMaxIdle(long idle) {
        if (idle <= 0 || idle == Long.MAX_VALUE) {
            maxIdleMillis = Long.MAX_VALUE;
        } else {
            maxIdleMillis = idle;
        }
    }

    public void setExpirationTime(final long expTime) {
        if (expTime <= 0) {
            this.expirationTime = Long.MAX_VALUE;
        } else {
            this.expirationTime = expTime;
        }
    }

    public void setTTL(long ttl) {
        if (ttl <= 0 || ttl == Long.MAX_VALUE) {
            setExpirationTime(Long.MAX_VALUE);
        } else {
            setExpirationTime(Clock.currentTimeMillis() + ttl);
        }
    }

    public void setInvalid() {
        expirationTime = (Clock.currentTimeMillis() - 10);
    }

    public boolean isValid(long now) {
        if (expirationTime == Long.MAX_VALUE && maxIdleMillis == Long.MAX_VALUE) {
            return true;
        }
        long lastTouch = Math.max(lastUpdateTime, Math.max(lastAccessTime, creationTime));
        long idle = now - lastTouch;
        return expirationTime > now && (maxIdleMillis > idle);
    }

    public boolean isValid() {
        return active && isValid(Clock.currentTimeMillis());
    }

    public void markRemoved() {
        setActive(false);
        setRemoveTime(Clock.currentTimeMillis());
    }

    public void setActive() {
        setRemoveTime(0);
        setActive(true);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void incrementVersion() {
        this.version++;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long newValue) {
        creationTime = newValue;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public int getHits() {
        return hits;
    }

    public void incrementHits() {
        hits++;
    }

    public void setActive(boolean active) {
        this.active = active;
        invalidateValueCache();
    }

    public Collection<ValueHolder> getMultiValues() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().lsMultiValues;
    }

    public void setMultiValues(Collection<ValueHolder> lsValues) {
        if (lsValues != null || optionalInfo != null) {
            this.getOptionalInfo().lsMultiValues = lsValues;
        }
    }

    public int getBackupOpCount() {
        if (optionalInfo == null) return 0;
        return (getOptionalInfo().backupOps == null) ? 0 : getOptionalInfo().backupOps.size();
    }

    public SortedSet<VersionedBackupOp> getBackupOps() {
        return getOptionalInfo().backupOps;
    }

    public void setBackupOps(SortedSet<VersionedBackupOp> backupOps) {
        if (backupOps != null) {
            this.getOptionalInfo().backupOps = backupOps;
        }
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    public long getRemoveTime() {
        return removeTime;
    }

    public void setRemoveTime(long removeTime) {
        this.removeTime = removeTime;
    }

    public boolean hasScheduledAction() {
        return optionalInfo != null && optionalInfo.lsScheduledActions != null &&
               optionalInfo.lsScheduledActions.size() > 0;
    }

    public List<ScheduledAction> getScheduledActions() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().lsScheduledActions;
    }

    public void setScheduledActions(List<ScheduledAction> lsScheduledActions) {
        if (lsScheduledActions != null) {
            this.getOptionalInfo().lsScheduledActions = lsScheduledActions;
        }
    }

    public Map<Address, Boolean> getListeners() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().mapListeners;
    }

    public void setMapListeners(Map<Address, Boolean> mapListeners) {
        if (mapListeners != null) {
            this.getOptionalInfo().mapListeners = mapListeners;
        }
    }

    public int getScheduledActionCount() {
        if (optionalInfo == null) return 0;
        return (getOptionalInfo().lsScheduledActions == null) ? 0 : getOptionalInfo().lsScheduledActions.size();
    }

    public OptionalInfo getOptionalInfo() {
        if (optionalInfo == null) {
            optionalInfo = new OptionalInfo();
        }
        return optionalInfo;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public boolean isRemoved() {
        return !active && removeTime > 0;
    }

    /**
     * True if record is not removed (map.remove() ...)
     * and either is not active or not valid or has not value (may because of locking)
     */
    public boolean isLoadable() {
        return !isRemoved() && (!isActive() || !isValid() || !hasValueData());
    }

    public int hashCode() {
        return super.hashCode();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractRecord)) return false;
        Record record = (Record) o;
        return record.getId() == getId();
    }

    public String toString() {
        return "Record key=" + getKeyData() + ", active=" + isActive()
               + ", version=" + getVersion() + ", removable=" + isRemovable()
               + ", evictable=" + isEvictable() + ", valueCount= " + valueCount()
               + ", isLocked= " + isLocked() + ", scheduled= " + getScheduledActionCount()
               ;

    }

    class OptionalInfo {

        volatile Collection<ValueHolder> lsMultiValues = null; // multimap values
        Long[] indexes; // indexes of the current value;
        byte[] indexTypes; // index types of the current value;
        List<ScheduledAction> lsScheduledActions = null;
        SortedSet<VersionedBackupOp> backupOps = null;
        Map<Address, Boolean> mapListeners = null;
    }
}
