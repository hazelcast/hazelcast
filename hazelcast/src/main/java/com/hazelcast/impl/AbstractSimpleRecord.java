/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.Collection;
import java.util.Map;

public abstract class AbstractSimpleRecord implements Record {
    protected final long id;
    protected final Data key;
    protected final short blockId;
    protected volatile boolean active = true;

    public AbstractSimpleRecord(int blockId, long id, Data key) {
        this.blockId = (short) blockId;
        this.id = id;
        this.key = key;
    }

    public void runBackupOps() {
    }

    public void forceBackupOps() {
    }

    public Object getKey() {
        return null;
    }

    public Data getKeyData() {
        return key;
    }

    public Long[] getIndexes() {
        return null;
    }

    public byte[] getIndexTypes() {
        return null;
    }

    public void setIndexes(Long[] indexes, byte[] indexTypes) {
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

//    public void addScheduledAction(ScheduledAction scheduledAction) {
//    }

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

    public void setExpirationTime(final long expirationTime) {
    }

    public void setTTL(long ttl) {
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
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getName() {
//        return cmap.name;
        return null;
    }

    public int getBlockId() {
        return blockId;
    }

    public DistributedLock getLock() {
        return null;
    }

    public void setLock(DistributedLock lock) {
    }

    public Collection<ValueHolder> getMultiValues() {
        return null;
    }

    public void setMultiValues(Collection<ValueHolder> lsValues) {
    }

    public int getBackupOpCount() {
        return 0;
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

    public long getId() {
        return id;
    }

    public boolean hasScheduledAction() {
        return false;
    }

//    public List<ScheduledAction> getScheduledActions() {
//        return null;
//    }
//
//    public void setScheduledActions(List<ScheduledAction> lsScheduledActions) {
//    }

    public Map<Address, Boolean> getListeners() {
        return null;
    }

    public void setMapListeners(Map<Address, Boolean> mapListeners) {
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

    public long getLockAcquireTime() {
        return -1L;
    }

    public AbstractRecord.OptionalInfo getOptionalInfo() {
        return null;
    }

    public void setLastStoredTime(long lastStoredTime) {
    }

    public long getLastStoredTime() {
        return 0;
    }

    public boolean isRemoved() {
        return !active;
    }

    public boolean isLoadable() {
        return !isActive() || !isValid() || !hasValueData();
    }

    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
