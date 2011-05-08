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

package com.hazelcast.impl;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.util.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.IOUtil.toObject;

public final class Record implements MapEntry {

    private final CMap cmap;
    private final Long id;
    private final short blockId;
    private final Data key;

    private volatile Object valueObject = null;
    private volatile Data value;
    private volatile boolean active = true;
    private volatile int hits = 0;
    private volatile long version = 0;
    private volatile long maxIdleMillis = Long.MAX_VALUE;
    private volatile long writeTime = -1;
    private volatile long removeTime = 0;
    private volatile long lastAccessTime = 0;
    private volatile long lastStoredTime = 0;
    private volatile long creationTime = 0;
    private volatile long expirationTime = Long.MAX_VALUE;
    private volatile long lastUpdateTime = 0;
    private volatile boolean dirty = false;
    private volatile int copyCount = 0;

    private volatile DistributedLock lock = null;

    private OptionalInfo optionalInfo = null;

    public Record(CMap cmap, int blockId, Data key, Data value, long ttl, long maxIdleMillis, Long id) {
        super();
        this.id = id;
        this.cmap = cmap;
        this.blockId = (short) blockId;
        this.key = key;
        this.value = value;
        this.setCreationTime(System.currentTimeMillis());
        this.setExpirationTime(ttl);
        this.maxIdleMillis = (maxIdleMillis == 0) ? Long.MAX_VALUE : maxIdleMillis;
        this.setVersion(0);
    }

    public long getLastTouchTime() {
        return Math.max(lastAccessTime, Math.max(creationTime, lastUpdateTime));
    }

    public Record copy() {
        Record recordCopy = new Record(cmap, blockId, key, value, getRemainingTTL(), getRemainingIdle(), id);
        if (optionalInfo != null) {
            recordCopy.setIndexes(getOptionalInfo().indexes, getOptionalInfo().indexTypes);
            recordCopy.setMultiValues(getOptionalInfo().lsMultiValues);
        }
        if (lock != null) {
            recordCopy.setLock(new DistributedLock(lock));
        }
        recordCopy.setCopyCount(copyCount);
        recordCopy.setVersion(getVersion());
        return recordCopy;
    }

    public void runBackupOps() {
        if (getBackupOps() != null) {
            if (getBackupOps().size() > 0) {
                Iterator<VersionedBackupOp> it = getBackupOps().iterator();
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

    public Object getValue() {
        final Object currentValue = valueObject;
        if (currentValue != null) {
            return currentValue;
        }
        synchronized (Record.this) {
            if (valueObject != null) {
                return valueObject;
            }
            Object v = toObject(value);
            if (cmap.cacheValue) {
                valueObject = v;
            }
            return v;
        }
    }

    public Data getKeyData() {
        return key;
    }

    public Data getValueData() {
        return value;
    }

    public Object setValue(Object value) {
        Object oldValue = getValue();
        valueObject = value;
        return oldValue;
    }

    public void setValue(Data value) {
        invalidateValueCache();
        this.value = value;
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

    public int valueCount() {
        int count = 0;
        if (getValueData() != null) {
            count = 1;
        } else if (getMultiValues() != null) {
            count = getMultiValues().size();
        } else if (copyCount > 0) {
            count += copyCount;
        }
        return count;
    }

    public long getCost() {
        long cost = 0;
        // avoid race condition with local references
        final Data dataValue = getValueData();
        final Data dataKey = getKeyData();
        if (dataValue != null) {
            cost = dataValue.size();
            if (copyCount > 0) {
                cost *= copyCount;
            }
            if (valueObject != null) {
                cost += dataValue.size();
            }
        } else if (getMultiValues() != null && getMultiValues().size() > 0) {
            for (Data data : getMultiValues()) {
                if (data != null) {
                    cost += data.size();
                }
            }
        }
        return cost + dataKey.size() + 403;
    }

    public boolean containsValue(Data value) {
        if (this.getValueData() != null) {
            return this.getValueData().equals(value);
        } else if (getMultiValues() != null) {
            int count = getMultiValues().size();
            if (count > 0) {
                return getMultiValues().contains(value);
            }
        }
        return false;
    }

    public void addValue(Data value) {
        if (value != null) {
            if (getMultiValues() == null) {
                setMultiValues(new ConcurrentHashSet<Data>() {
                    @Override
                    public boolean add(Data e) {
                        return e != null && super.add(e);
                    }
                });
            }
            getMultiValues().add(value);
        }
    }

    public boolean unlock(int threadId, Address address) {
        invalidateValueCache();
        return lock == null || lock.unlock(address, threadId);
    }

    public boolean testLock(int threadId, Address address) {
        return lock == null || lock.testLock(threadId, address);
    }

    public boolean lock(int threadId, Address address) {
        invalidateValueCache();
        if (lock == null) {
            lock = new DistributedLock(address, threadId);
            return true;
        }
        return lock.lock(address, threadId);
    }

    private void invalidateValueCache() {
        valueObject = null;
    }

    public void addScheduledAction(ScheduledAction scheduledAction) {
        if (getScheduledActions() == null) {
            setScheduledActions(new LinkedList<ScheduledAction>());
        }
        getScheduledActions().add(scheduledAction);
    }

    public boolean isRemovable() {
        return !isActive() && (valueCount() <= 0 && !hasListener() && (getScheduledActionCount() == 0) && getBackupOpCount() == 0);
    }

    public boolean isEvictable() {
        return (getLockCount() == 0 && !hasListener() && (getScheduledActionCount() == 0));
    }

    public boolean hasListener() {
        return (getListeners() != null && getListeners().size() > 0);
    }

    public void addListener(Address address, boolean returnValue) {
        if (getListeners() == null)
            setMapListeners(new HashMap<Address, Boolean>(1));
        getListeners().put(address, returnValue);
    }

    public void removeListener(Address address) {
        if (getListeners() == null)
            return;
        getListeners().remove(address);
    }

    public void incrementCopyCount() {
        copyCount++;
    }

    public void decrementCopyCount() {
        copyCount--;
    }

    public int getCopyCount() {
        return copyCount;
    }

    public void setLastUpdated() {
        if (expirationTime != Long.MAX_VALUE && expirationTime > 0) {
            long ttl = expirationTime - (lastUpdateTime > 0L ? lastUpdateTime : creationTime);
            setExpirationTime(ttl);
        }
        setLastUpdateTime(System.currentTimeMillis());
    }

    public void setLastAccessed() {
        setLastAccessTime(System.currentTimeMillis());
        incrementHits();
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public long getRemainingTTL() {
        if (expirationTime == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        } else {
            long ttl = expirationTime - System.currentTimeMillis();
            return (ttl < 0) ? 1 : ttl;
        }
    }

    public long getRemainingIdle() {
        if (maxIdleMillis == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        } else {
            long lastTouch = Math.max(lastAccessTime, creationTime);
            long idle = System.currentTimeMillis() - lastTouch;
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

    public void setExpirationTime(long ttl) {
        if (ttl <= 0 || ttl == Long.MAX_VALUE) {
            expirationTime = Long.MAX_VALUE;
        } else {
            expirationTime = System.currentTimeMillis() + ttl;
        }
    }

    public void setInvalid() {
        expirationTime = (System.currentTimeMillis() - 10);
    }

    public boolean isValid(long now) {
        if (expirationTime == Long.MAX_VALUE && maxIdleMillis == Long.MAX_VALUE) {
            return true;
        }
        long lastTouch = Math.max(lastAccessTime, creationTime);
        long idle = now - lastTouch;
        return expirationTime > now && (maxIdleMillis > idle);
    }

    public boolean isValid() {
        return isValid(System.currentTimeMillis());
    }

    public void markRemoved() {
        setActive(false);
        setRemoveTime(System.currentTimeMillis());
    }

    public void setActive() {
        setRemoveTime(0);
        setActive(true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record)) return false;
        Record record = (Record) o;
        return record.getId().equals(getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public String toString() {
        return "Record key=" + getKeyData() + ", active=" + isActive()
                + ", version=" + getVersion() + ", removable=" + isRemovable();
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

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
        invalidateValueCache();
    }

    public String getName() {
        return cmap.name;
    }

    public int getBlockId() {
        return blockId;
    }

    public DistributedLock getLock() {
        return lock;
    }

    public void setLock(DistributedLock lock) {
        this.lock = lock;
    }

    public Set<Data> getMultiValues() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().lsMultiValues;
    }

    public void setMultiValues(Set<Data> lsValues) {
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

    public Long getId() {
        return id;
    }

    public boolean hasScheduledAction() {
        return optionalInfo != null && optionalInfo.lsScheduledActions != null && optionalInfo.lsScheduledActions.size() > 0;
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

    public void setCopyCount(int copyCount) {
        this.copyCount = copyCount;
    }

    public boolean isLocked() {
        return lock != null && lock.isLocked();
    }

    public int getScheduledActionCount() {
        if (optionalInfo == null) return 0;
        return (getOptionalInfo().lsScheduledActions == null) ? 0 : getOptionalInfo().lsScheduledActions.size();
    }

    public int getLockCount() {
        return (lock == null) ? 0 : lock.getLockCount();
    }

    public void clearLock() {
        if (lock != null) {
            lock.clear();
        }
    }

    public Address getLockAddress() {
        return (lock == null) ? null : lock.getLockAddress();
    }

    public OptionalInfo getOptionalInfo() {
//        if (true) throw new RuntimeException();
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

    class OptionalInfo {
        private Long[] indexes; // indexes of the current value;
        private byte[] indexTypes; // index types of the current value;
        private List<ScheduledAction> lsScheduledActions = null;
        private Map<Address, Boolean> mapListeners = null;
        private Set<Data> lsMultiValues = null; // multimap values

        private SortedSet<VersionedBackupOp> backupOps = null;
    }
}
