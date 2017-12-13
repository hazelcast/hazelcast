/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * MultiMap container which holds a map of {@link MultiMapValue}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class MultiMapContainer extends MultiMapContainerSupport {

    private static final int ID_PROMOTION_OFFSET = 100000;

    private final DistributedObjectNamespace lockNamespace;
    private final LockStore lockStore;
    private final int partitionId;
    private final long creationTime;
    private final ObjectNamespace objectNamespace;

    private long idGen;

    // these fields are volatile since they can be read by other threads than the partition-thread
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;

    public MultiMapContainer(String name, MultiMapService service, int partitionId) {
        super(name, service.getNodeEngine());
        this.partitionId = partitionId;
        this.lockNamespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name);
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null : lockService.createLockStore(partitionId, lockNamespace);
        this.creationTime = currentTimeMillis();
        this.objectNamespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name);
    }

    public boolean canAcquireLock(Data dataKey, String caller, long threadId) {
        return lockStore != null && lockStore.canAcquireLock(dataKey, caller, threadId);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean isTransactionallyLocked(Data key) {
        return lockStore != null && lockStore.shouldBlockReads(key);
    }

    public boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads) {
        return lockStore != null && lockStore.txnLock(key, caller, threadId, referenceId, ttl, blockReads);
    }

    public boolean unlock(Data key, String caller, long threadId, long referenceId) {
        return lockStore != null && lockStore.unlock(key, caller, threadId, referenceId);
    }

    public boolean forceUnlock(Data key) {
        return lockStore != null && lockStore.forceUnlock(key);
    }

    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public String getLockOwnerInfo(Data dataKey) {
        return lockStore != null ? lockStore.getOwnerInfo(dataKey) : null;
    }

    public long nextId() {
        return idGen++;
    }

    public void setId(long newValue) {
        idGen = newValue + ID_PROMOTION_OFFSET;
    }

    public void delete(Data dataKey) {
        multiMapValues.remove(dataKey);
    }

    public Collection<MultiMapRecord> remove(Data dataKey, boolean copyOf) {
        MultiMapValue multiMapValue = multiMapValues.remove(dataKey);
        return multiMapValue != null ? multiMapValue.getCollection(copyOf) : null;
    }

    public Set<Data> keySet() {
        Set<Data> keySet = multiMapValues.keySet();
        return new HashSet<Data>(keySet);
    }

    public Collection<MultiMapRecord> values() {
        Collection<MultiMapRecord> valueCollection = new LinkedList<MultiMapRecord>();
        for (MultiMapValue multiMapValue : multiMapValues.values()) {
            valueCollection.addAll(multiMapValue.getCollection(false));
        }
        return valueCollection;
    }

    public boolean containsKey(Data key) {
        return multiMapValues.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        MultiMapValue multiMapValue = multiMapValues.get(key);
        if (multiMapValue == null) {
            return false;
        }
        MultiMapRecord record = new MultiMapRecord(binary ? value : nodeEngine.toObject(value));
        return multiMapValue.getCollection(false).contains(record);
    }

    public boolean containsValue(boolean binary, Data value) {
        for (Data key : multiMapValues.keySet()) {
            if (containsEntry(binary, key, value)) {
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection<MultiMapRecord>> copyCollections() {
        Map<Data, Collection<MultiMapRecord>> map = createHashMap(multiMapValues.size());
        for (Map.Entry<Data, MultiMapValue> entry : multiMapValues.entrySet()) {
            Data key = entry.getKey();
            Collection<MultiMapRecord> col = entry.getValue().getCollection(true);
            map.put(key, col);
        }
        return map;
    }

    public int size() {
        int size = 0;
        for (MultiMapValue multiMapValue : multiMapValues.values()) {
            size += multiMapValue.getCollection(false).size();
        }
        return size;
    }

    public int clear() {
        Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        Map<Data, MultiMapValue> lockedKeys = createHashMap(locks.size());
        for (Data key : locks) {
            MultiMapValue multiMapValue = multiMapValues.get(key);
            if (multiMapValue != null) {
                lockedKeys.put(key, multiMapValue);
            }
        }
        int numberOfAffectedEntries = multiMapValues.size() - lockedKeys.size();
        multiMapValues.clear();
        multiMapValues.putAll(lockedKeys);
        return numberOfAffectedEntries;
    }

    public void destroy() {
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionId, lockNamespace);
        }
        multiMapValues.clear();
    }

    public void access() {
        lastAccessTime = currentTimeMillis();
    }

    public void update() {
        lastUpdateTime = currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLockedCount() {
        return lockStore.getLockedKeys().size();
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }
}
