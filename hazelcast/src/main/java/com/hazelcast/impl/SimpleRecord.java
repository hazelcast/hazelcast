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

import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public class SimpleRecord implements Record {
    final CMap cmap;
    final long id;
    final Data key;
    final short blockId;
    Data value;
    private volatile boolean active = true;

    public SimpleRecord(int blockId, CMap cmap, long id, Data key, Data value) {
        this.blockId = (short) blockId;
        this.cmap = cmap;
        this.id = id;
        this.key = key;
        this.value = value;
    }

    public long getLastTouchTime() {
        return 0;
    }

    public Record copy() {
        return new SimpleRecord(blockId, cmap, id, key, value);
    }

    public void runBackupOps() {
    }

    public void addBackupOp(VersionedBackupOp bo) {
    }

    public void forceBackupOps() {
    }

    public Object getKey() {
        return null;
    }

    public Object getValue() {
        return null;
    }

    public Data getKeyData() {
        return key;
    }

    public Data getValueData() {
        return value;
    }

    public Object setValue(Object value) {
        return null;
    }

    public void setValue(Data value) {
        this.value = value;
    }

    public Long[] getIndexes() {
        return null;
    }

    public byte[] getIndexTypes() {
        return null;
    }

    public void setIndexes(Long[] indexes, byte[] indexTypes) {
    }

    public int valueCount() {
        return 1;
    }

    public long getCost() {
        return key.size() + value.size() + 30;
    }

    public boolean containsValue(Data value) {
        return false;
    }

    public void addValue(Data value) {
    }

    public boolean unlock(int threadId, Address address) {
        return false;
    }

    public boolean testLock(int threadId, Address address) {
        return true;
    }

    public boolean lock(int threadId, Address address) {
        return false;
    }

    public void addScheduledAction(ScheduledAction scheduledAction) {
    }

    public boolean isRemovable() {
        return false;
    }

    public boolean isEvictable() {
        return false;
    }

    public boolean hasListener() {
        return false;
    }

    public void addListener(Address address, boolean returnValue) {
    }

    public void removeListener(Address address) {
    }

    public void incrementCopyCount() {
    }

    public void decrementCopyCount() {
    }

    public int getCopyCount() {
        return 0;
    }

    public void setLastUpdated() {
    }

    public void setLastAccessed() {
    }

    public long getExpirationTime() {
        return 0;
    }

    public long getRemainingTTL() {
        return 0;
    }

    public long getRemainingIdle() {
        return 0;
    }

    public void setMaxIdle(long idle) {
    }

    public void setExpirationTime(long ttl) {
    }

    public void setInvalid() {
        active = false;
    }

    public boolean isValid(long now) {
        return active;
    }

    public boolean isValid() {
        return active;
    }

    public void markRemoved() {
        active = false;
    }

    public void setActive() {
        active = true;
    }

    public long getVersion() {
        return 0;
    }

    public void setVersion(long version) {
    }

    public void incrementVersion() {
    }

    public long getCreationTime() {
        return 0;
    }

    public void setCreationTime(long newValue) {
    }

    public long getLastAccessTime() {
        return 0;
    }

    public void setLastAccessTime(long lastAccessTime) {
    }

    public long getLastUpdateTime() {
        return 0;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
    }

    public int getHits() {
        return 0;
    }

    public void incrementHits() {
    }

    public boolean isActive() {
        return false;
    }

    public void setActive(boolean active) {
    }

    public String getName() {
        return cmap.name;
    }

    public int getBlockId() {
        return blockId;
    }

    public DistributedLock getLock() {
        return null;
    }

    public void setLock(DistributedLock lock) {
    }

    public Set<Data> getMultiValues() {
        return null;
    }

    public void setMultiValues(Set<Data> lsValues) {
    }

    public int getBackupOpCount() {
        return 0;
    }

    public SortedSet<VersionedBackupOp> getBackupOps() {
        return null;
    }

    public void setBackupOps(SortedSet<VersionedBackupOp> backupOps) {
    }

    public boolean isDirty() {
        return false;
    }

    public void setDirty(boolean dirty) {
    }

    public long getWriteTime() {
        return 0;
    }

    public void setWriteTime(long writeTime) {
    }

    public long getRemoveTime() {
        return 0;
    }

    public void setRemoveTime(long removeTime) {
    }

    public Long getId() {
        return id;
    }

    public boolean hasScheduledAction() {
        return false;
    }

    public List<ScheduledAction> getScheduledActions() {
        return null;
    }

    public void setScheduledActions(List<ScheduledAction> lsScheduledActions) {
    }

    public Map<Address, Boolean> getListeners() {
        return null;
    }

    public void setMapListeners(Map<Address, Boolean> mapListeners) {
    }

    public void setCopyCount(int copyCount) {
    }

    public boolean isLocked() {
        return false;
    }

    public int getScheduledActionCount() {
        return 0;
    }

    public int getLockCount() {
        return 0;
    }

    public void clearLock() {
    }

    public Address getLockAddress() {
        return null;
    }

    public AbstractRecord.OptionalInfo getOptionalInfo() {
        return null;
    }

    public void setLastStoredTime(long lastStoredTime) {
    }

    public long getLastStoredTime() {
        return 0;
    }

    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    public boolean equals(Object obj) {
        return obj instanceof SimpleRecord && id == ((SimpleRecord) obj).id;
    }
}
