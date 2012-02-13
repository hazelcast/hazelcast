/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public interface Record extends MapEntry {

    /**
     * @return on-heap copy of Record
     */
    Record copy();

    void runBackupOps();

    void addBackupOp(VersionedBackupOp bo);

    void forceBackupOps();

    Object getKey();

    Object getValue();

    Data getKeyData();

    Data getValueData();

    Object setValue(Object value);

    void setValueData(Data value);

    Long[] getIndexes();

    byte[] getIndexTypes();

    void setIndexes(Long[] indexes, byte[] indexTypes);

    int valueCount();

    long getCost();

    boolean containsValue(Data value);

    boolean unlock(int threadId, Address address);

    boolean testLock(int threadId, Address address);

    boolean lock(int threadId, Address address);

    void addScheduledAction(ScheduledAction scheduledAction);

    boolean isRemovable();

    boolean isEvictable();

    boolean hasListener();

    void addListener(Address address, boolean returnValue);

    void removeListener(Address address);

    void setLastUpdated();

    void setLastAccessed();

    long getExpirationTime();

    long getRemainingTTL();

    long getRemainingIdle();

    void setMaxIdle(long idle);

    void setExpirationTime(long ttl);

    void setInvalid();

    boolean isValid(long now);

    boolean isValid();

    void markRemoved();

    void setActive();

    boolean equals(Object o);

    int hashCode();

    String toString();

    long getVersion();

    void setVersion(long version);

    void incrementVersion();

    long getCreationTime();

    void setCreationTime(long newValue);

    long getLastAccessTime();

    void setLastAccessTime(long lastAccessTime);

    long getLastUpdateTime();

    void setLastUpdateTime(long lastUpdateTime);

    int getHits();

    void incrementHits();

    boolean isActive();

    void setActive(boolean active);

    String getName();

    int getBlockId();

    DistributedLock getLock();

    void setLock(DistributedLock lock);

    Collection<ValueHolder> getMultiValues();

    void setMultiValues(Collection<ValueHolder> lsValues);

    int getBackupOpCount();

    SortedSet<VersionedBackupOp> getBackupOps();

    void setBackupOps(SortedSet<VersionedBackupOp> backupOps);

    boolean isDirty();

    void setDirty(boolean dirty);

    long getWriteTime();

    void setWriteTime(long writeTime);

    long getRemoveTime();

    void setRemoveTime(long removeTime);

    long getId();

    boolean hasScheduledAction();

    List<ScheduledAction> getScheduledActions();

    void setScheduledActions(List<ScheduledAction> lsScheduledActions);

    Map<Address, Boolean> getListeners();

    void setMapListeners(Map<Address, Boolean> mapListeners);

    boolean isLocked();

    int getScheduledActionCount();

    int getLockCount();

    void clearLock();

    Address getLockAddress();

    AbstractRecord.OptionalInfo getOptionalInfo();

    void setLastStoredTime(long lastStoredTime);

    long getLastStoredTime();

    boolean hasValueData();

    void invalidate();
}
