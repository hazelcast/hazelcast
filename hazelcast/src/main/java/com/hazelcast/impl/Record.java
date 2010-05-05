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

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Record implements MapEntry {
    private static final Logger logger = Logger.getLogger(Record.class.getName());

    private final CMap cmap;
    private final long id;
    private final int blockId;
    private final Data key;

    private volatile Data value;
    private volatile boolean active = true;
    private volatile int hits = 0;
    private volatile long version = 0;
    private volatile long maxIdleMillis = Long.MAX_VALUE;
    private volatile long writeTime = -1;
    private volatile long removeTime = 0;
    private volatile long lastAccessTime = 0;
    private volatile long creationTime = 0;
    private volatile long expirationTime = Long.MAX_VALUE;
    private volatile long lastUpdateTime = 0;
    private volatile boolean dirty = false;
    private volatile int copyCount = 0;

    private DistributedLock lock = null;

    private OptionalInfo optionalInfo = null;

    public Record(CMap cmap, int blockId, Data key, Data value, long ttl, long maxIdleMillis, long id) {
        super();
        this.id = id;
        this.cmap = cmap;
        this.blockId = blockId;
        this.key = key;
        this.value = value;
        this.setCreationTime(System.currentTimeMillis());
        this.setExpirationTime(ttl);
        this.maxIdleMillis = (maxIdleMillis == 0) ? Long.MAX_VALUE : maxIdleMillis;
        this.setVersion(0);
    }

    public Record copy() {
        Record recordCopy = new Record(cmap, blockId, key, value, getRemainingTTL(), getRemainingIdle(), id);
        recordCopy.setIndexes(getOptionalInfo().indexes, getOptionalInfo().indexTypes);
        if (lock != null) {
            recordCopy.setLock(new DistributedLock(lock));
        }
        recordCopy.setMultiValues(getOptionalInfo().lsMultiValues);
        recordCopy.setCopyCount(copyCount);
        recordCopy.setVersion(getVersion());
        return recordCopy;
    }

    public RecordEntry getRecordEntry() {
        return cmap.getRecordEntry(this);
    }

    public CMap getCMap() {
        return cmap;
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
        return key;
    }

    public Data getValue() {
        return value;
    }

    public Object setValue(Object value) {
        return getRecordEntry().setValue(value);
    }

    public void setValue(Data value) {
        cmap.invalidateRecordEntryValue(this);
        this.value = value;
    }

    public long[] getIndexes() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().indexes;
    }

    public byte[] getIndexTypes() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().indexTypes;
    }

    public void setIndexes(long[] indexes, byte[] indexTypes) {
        if (indexes != null) {
            this.getOptionalInfo().indexes = indexes;
            this.getOptionalInfo().indexTypes = indexTypes;
        }
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
        return lock == null || lock.unlock(address, threadId);
    }

    public boolean testLock(int threadId, Address address) {
        return lock == null || lock.testLock(threadId, address);
    }

    public boolean lock(int threadId, Address address) {
        if (lock == null) {
            lock = new DistributedLock(address, threadId);
            return true;
        }
        return lock.lock(address, threadId);
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
        return (getLockCount() == 0 && !hasListener() && (getScheduledActionCount() == 0));
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
        return "Record key=" + getKey() + ", active=" + isActive()
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
        if (!active) {
            cmap.invalidateRecordEntryValue(this);
        }
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
        return (getOptionalInfo().backupOps == null) ? 0 : getOptionalInfo().backupOps.size();
    }

    public SortedSet<VersionedBackupOp> getBackupOps() {
        return getOptionalInfo().backupOps;
    }

    public void setBackupOps(SortedSet<VersionedBackupOp> backupOps) {
        this.getOptionalInfo().backupOps = backupOps;
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

    public List<ScheduledAction> getScheduledActions() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().lsScheduledActions;
    }

    public void setScheduledActions(List<ScheduledAction> lsScheduledActions) {
        this.getOptionalInfo().lsScheduledActions = lsScheduledActions;
    }

    public Map<Address, Boolean> getMapListeners() {
        if (optionalInfo == null) return null;
        return getOptionalInfo().mapListeners;
    }

    public void setMapListeners(Map<Address, Boolean> mapListeners) {
        this.getOptionalInfo().mapListeners = mapListeners;
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

    class OptionalInfo {
        private long[] indexes; // indexes of the current value;
        private byte[] indexTypes; // index types of the current value;
        private List<ScheduledAction> lsScheduledActions = null;
        private Map<Address, Boolean> mapListeners = null;
        private Set<Data> lsMultiValues = null; // multimap values

        private SortedSet<VersionedBackupOp> backupOps = null;
    }
}
