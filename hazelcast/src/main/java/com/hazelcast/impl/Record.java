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
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hazelcast.nio.IOUtil.toObject;

public class Record implements MapEntry {
    private static final Logger logger = Logger.getLogger(Record.class.getName());

    private final RecordEntry recordEntry;

    private final FactoryImpl factory;
    private final long id;
    private final String name;
    private final int blockId;
    private final AtomicReference<Data> key = new AtomicReference<Data>();
    private final AtomicReference<Data> value = new AtomicReference<Data>();
    private final AtomicLong version = new AtomicLong();
    private final AtomicInteger hits = new AtomicInteger(0);

    private volatile boolean active = true;
    private volatile long maxIdleMillis = Long.MAX_VALUE;
    private volatile long writeTime = -1;
    private volatile long removeTime =0;
    private volatile long lastAccessTime = 0;
    private volatile long creationTime = 0;
    private volatile long lastTouchTime = 0;
    private volatile long expirationTime = Long.MAX_VALUE;
    private volatile long lastUpdateTime = 0;
    private volatile boolean dirty = false;
    private volatile int copyCount = 0;

    private long[] indexes; // indexes of the current value;
    private byte[] indexTypes; // index types of the current value;

    private Address lockAddress = null;
    private int lockThreadId = -1;
    private int lockCount = 0;
    private List<ScheduledAction> lsScheduledActions = null;
    private Map<Address, Boolean> mapListeners = null;
    private Set<Data> lsMultiValues = null; // multimap values

    private SortedSet<VersionedBackupOp> backupOps = null;

    public Record(FactoryImpl factory, String name, int blockId, Data key, Data value, long ttl, long maxIdleMillis, long id) {
        super();
        this.recordEntry = new RecordEntry(this);
        this.factory = factory;
        this.name = name;
        this.blockId = blockId;
        this.setKey(key);
        this.setValue(value);
        this.setCreationTime(System.currentTimeMillis());
        this.setExpirationTime(ttl);
        this.maxIdleMillis = (maxIdleMillis == 0) ? Long.MAX_VALUE : maxIdleMillis;
        this.setLastTouchTime(getCreationTime());
        this.setVersion(0);
        this.id = id;
    }

    public Record copy() {
        Record recordCopy = new Record(factory, name, blockId, key.get(), value.get(), getRemainingTTL(), getRemainingIdle(), id);
        recordCopy.setIndexes(indexes, indexTypes);
        recordCopy.setLockCount(lockCount);
        recordCopy.setLockAddress(lockAddress);
        recordCopy.setLockThreadId(lockThreadId);
        recordCopy.setMultiValues(lsMultiValues);
        recordCopy.setCopyCount(copyCount);
        recordCopy.setVersion(getVersion());
        return recordCopy;
    }

    public RecordEntry getRecordEntry() {
        return recordEntry;
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
            logger.log(Level.FINEST, " Forcing backup.run version " + getVersion());
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

    public Data getKey() {
        return key.get();
    }

    public void setKey(Data key) {
        this.key.set(key);
    }

    public Data getValue() {
        return value.get();
    }

    public Object setValue(Object value) {
        return getRecordEntry().setValue(value);
    }

    public void setValue(Data value) {
        recordEntry.setValueObject(null);
        this.value.set(value);
    }

    public long[] getIndexes() {
        return indexes;
    }

    public byte[] getIndexTypes() {
        return indexTypes;
    }

    public void setIndexes(long[] indexes, byte[] indexTypes) {
        this.indexes = indexes;
        this.indexTypes = indexTypes;
    }

    public int valueCount() {
        int count = 0;
        if (getValue() != null) {
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
        if (getValue() != null) {
            cost = getValue().size();
            if (copyCount > 0) {
                cost *= copyCount;
            }
        } else if (getMultiValues() != null) {
            for (Data data : getMultiValues()) {
                cost += data.size();
            }
        }
        return cost + getKey().size() + 250;
    }

    public boolean containsValue(Data value) {
        if (this.getValue() != null) {
            return this.getValue().equals(value);
        } else if (getMultiValues() != null) {
            int count = getMultiValues().size();
            if (count > 0) {
                return getMultiValues().contains(value);
            }
        }
        return false;
    }

    public void addValue(Data value) {
        if (getMultiValues() == null) {
            setMultiValues(new HashSet<Data>(2));
        }
        getMultiValues().add(value);
    }

    public boolean unlock(int threadId, Address address) {
        if (threadId == -1 || address == null)
            throw new IllegalArgumentException();
        if (lockCount == 0)
            return true;
        if (getLockThreadId() != threadId || !address.equals(getLockAddress())) {
            return false;
        }
        if (lockCount > 0) {
            lockCount--;
        }
        return true;
    }

    public boolean testLock(int threadId, Address address) {
        return lockCount == 0 || getLockThreadId() == threadId && getLockAddress().equals(address);
    }

    public boolean lock(int threadId, Address address) {
        if (lockCount == 0) {
            setLockThreadId(threadId);
            setLockAddress(address);
            lockCount++;
            return true;
        }
        if (getLockThreadId() == threadId && getLockAddress().equals(address)) {
            lockCount++;
            return true;
        }
        return false;
    }

    public void addScheduledAction(ScheduledAction scheduledAction) {
        if (getScheduledActions() == null) {
            setScheduledActions(new LinkedList<ScheduledAction>());
        }
        getScheduledActions().add(scheduledAction);
        logger.log(Level.FINEST, scheduledAction.getRequest().operation + " scheduling " + scheduledAction);
    }

    public boolean isRemovable() {
        return !isActive() && (valueCount() <= 0 && !hasListener() && (getScheduledActionCount() == 0) && getBackupOpCount() == 0);
    }

    public boolean isEvictable() {
        return (lockCount == 0 && !hasListener() && (getScheduledActionCount() == 0));
    }

    public boolean hasListener() {
        return (getMapListeners() != null && getMapListeners().size() > 0);
    }

    public void addListener(Address address, boolean returnValue) {
        if (getMapListeners() == null)
            setMapListeners(new HashMap<Address, Boolean>(1));
        getMapListeners().put(address, returnValue);
    }

    public void removeListener(Address address) {
        if (getMapListeners() == null)
            return;
        getMapListeners().remove(address);
    }

    public void incrementCopyCount() {
        copyCount += 1;
    }

    public void decrementCopyCount() {
        copyCount -= 1;
    }

    public int getCopyCount() {
        return copyCount;
    }

    public void setLastUpdated() {
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
            expirationTime = getCreationTime() + ttl;
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
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return (record.getId() == getId());
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    public String toString() {
        return "Record key=" + getKey() + ", removable=" + isRemovable();
    }

    public long getVersion() {
        return version.get();
    }

    public void setVersion(long version) {
        this.version.set(version);
    }

    public void incrementVersion() {
        this.version.incrementAndGet();
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
        return hits.get();
    }

    public void incrementHits() {
        this.hits.incrementAndGet();
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
        if (!active) {
            this.recordEntry.setValueObject(null);
        }
    }

    public String getName() {
        return name;
    }

    public int getBlockId() {
        return blockId;
    }

    public int getLockThreadId() {
        return lockThreadId;
    }

    public void setLockThreadId(int lockThreadId) {
        this.lockThreadId = lockThreadId;
    }

    public int getLockCount() {
        return lockCount;
    }

    public void setLockCount(int lockCount) {
        this.lockCount = lockCount;
    }

    public Address getLockAddress() {
        return lockAddress;
    }

    public void setLockAddress(Address lockAddress) {
        this.lockAddress = lockAddress;
    }

    public Set<Data> getMultiValues() {
        return lsMultiValues;
    }

    public void setMultiValues(Set<Data> lsValues) {
        this.lsMultiValues = lsValues;
    }

    public int getBackupOpCount() {
        return (backupOps == null) ? 0 : backupOps.size();
    }

    public SortedSet<VersionedBackupOp> getBackupOps() {
        return backupOps;
    }

    public void setBackupOps(SortedSet<VersionedBackupOp> backupOps) {
        this.backupOps = backupOps;
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

    public long getId() {
        return id;
    }

    public long getLastTouchTime() {
        return lastTouchTime;
    }

    public void setLastTouchTime(long lastTouchTime) {
        this.lastTouchTime = lastTouchTime;
    }

    public List<ScheduledAction> getScheduledActions() {
        return lsScheduledActions;
    }

    public void setScheduledActions(List<ScheduledAction> lsScheduledActions) {
        this.lsScheduledActions = lsScheduledActions;
    }

    public Map<Address, Boolean> getMapListeners() {
        return mapListeners;
    }

    public void setMapListeners(Map<Address, Boolean> mapListeners) {
        this.mapListeners = mapListeners;
    }

    public void setCopyCount(int copyCount) {
        this.copyCount = copyCount;
    }

    public boolean isLocked() {
        return lockCount > 0;
    }

    public int getScheduledActionCount() {
        return (lsScheduledActions == null) ? 0 : lsScheduledActions.size();
    }

    public static class RecordEntry implements MapEntry {

        private final Record record;
        private volatile Object keyObject;
        private volatile Object valueObject;

        RecordEntry(Record record) {
            this.record = record;
        }

        public boolean isValid() {
            return record.isActive();
        }

        public Object getKey() {
            if (keyObject == null) {
                keyObject = toObject(record.getKey());
            }
            return keyObject;
        }

        public Object getValue() {
            if (valueObject == null) {
                valueObject = toObject(record.getValue());
            }
            return valueObject;
        }

        void setValueObject(Object valueObject) {
            this.valueObject = valueObject;
        }

        public Object setValue(Object value) {
            MProxy proxy = (MProxy) record.factory.getOrCreateProxyByName(record.getName());
            Object oldValue = proxy.put(getKey(), value);
            this.valueObject = value;
            return oldValue;
        }

        public long getCost() {
            return record.getCost();
        }

        public long getExpirationTime() {
            return record.getExpirationTime();
        }

        public long getVersion() {
            return record.getVersion();
        }

        public long getCreationTime() {
            return record.getCreationTime();
        }

        public long getLastAccessTime() {
            return record.getLastAccessTime();
        }

        public long getLastUpdateTime() {
            return record.getLastUpdateTime();
        }

        public int getHits() {
            return record.getHits();
        }
    }
}
