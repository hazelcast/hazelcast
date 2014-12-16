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

package com.hazelcast.multimap.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Multi-map container.
 */
public class MultiMapContainer extends MultiMapContainerSupport {

    private static final AtomicLongFieldUpdater<MultiMapContainer> ID_GEN_UPDATER = AtomicLongFieldUpdater
            .newUpdater(MultiMapContainer.class, "idGen");
    private static final AtomicLongFieldUpdater<MultiMapContainer> LAST_ACCCESS_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(MultiMapContainer.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<MultiMapContainer> LAST_UPDATE_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(MultiMapContainer.class, "lastUpdateTime");

    private final DefaultObjectNamespace lockNamespace;

    private final LockStore lockStore;

    private final int partitionId;

    private final long creationTime;

    // These fields are never accessed directly but through the UPDATER above
    private volatile long idGen;
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;

    public MultiMapContainer(String name, MultiMapService service, int partitionId) {
        super(name, service.getNodeEngine());
        this.partitionId = partitionId;
        this.lockNamespace = new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name);
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null : lockService.createLockStore(partitionId, lockNamespace);
        this.creationTime = Clock.currentTimeMillis();
    }

    public boolean canAcquireLock(Data dataKey, String caller, long threadId) {
        return lockStore != null && lockStore.canAcquireLock(dataKey, caller, threadId);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean isTransactionallyLocked(Data key) {
        return lockStore != null && lockStore.isTransactionallyLocked(key);
    }

    public boolean txnLock(Data key, String caller, long threadId, long ttl) {
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, long threadId) {
        return lockStore != null && lockStore.unlock(key, caller, threadId);
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
        return ID_GEN_UPDATER.getAndIncrement(this);
    }

    public void delete(Data dataKey) {
        multiMapWrappers.remove(dataKey);
    }

    public Collection<MultiMapRecord> remove(Data dataKey, boolean copyOf) {
        MultiMapWrapper wrapper = multiMapWrappers.remove(dataKey);
        return wrapper != null ? wrapper.getCollection(copyOf) : null;
    }

    public Set<Data> keySet() {
        Set<Data> keySet = multiMapWrappers.keySet();
        Set<Data> keys = new HashSet<Data>(keySet.size());
        keys.addAll(keySet);
        return keys;
    }

    public Collection<MultiMapRecord> values() {
        Collection<MultiMapRecord> valueCollection = new LinkedList<MultiMapRecord>();
        for (MultiMapWrapper wrapper : multiMapWrappers.values()) {
            valueCollection.addAll(wrapper.getCollection(false));
        }
        return valueCollection;
    }

    public boolean containsKey(Data key) {
        return multiMapWrappers.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        MultiMapWrapper wrapper = multiMapWrappers.get(key);
        if (wrapper == null) {
            return false;
        }
        MultiMapRecord record = new MultiMapRecord(binary ? value : nodeEngine.toObject(value));
        return wrapper.getCollection(false).contains(record);
    }

    public boolean containsValue(boolean binary, Data value) {
        for (Data key : multiMapWrappers.keySet()) {
            if (containsEntry(binary, key, value)) {
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection<MultiMapRecord>> copyCollections() {
        Map<Data, Collection<MultiMapRecord>> map = new HashMap<Data, Collection<MultiMapRecord>>(multiMapWrappers.size());
        for (Map.Entry<Data, MultiMapWrapper> entry : multiMapWrappers.entrySet()) {
            Data key = entry.getKey();
            Collection<MultiMapRecord> col = entry.getValue().getCollection(true);
            map.put(key, col);
        }
        return map;
    }

    public int size() {
        int size = 0;
        for (MultiMapWrapper wrapper : multiMapWrappers.values()) {
            size += wrapper.getCollection(false).size();
        }
        return size;
    }

    public int clear() {
        final Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        Map<Data, MultiMapWrapper> lockedKeys = new HashMap<Data, MultiMapWrapper>(locks.size());
        for (Data key : locks) {
            MultiMapWrapper wrapper = multiMapWrappers.get(key);
            if (wrapper != null) {
                lockedKeys.put(key, wrapper);
            }
        }
        int numberOfAffectedEntries = multiMapWrappers.size() - lockedKeys.size();
        multiMapWrappers.clear();
        multiMapWrappers.putAll(lockedKeys);
        return numberOfAffectedEntries;
    }

    public void destroy() {
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionId, lockNamespace);
        }
        multiMapWrappers.clear();
    }

    public void access() {
        LAST_ACCCESS_TIME_UPDATER.set(this, Clock.currentTimeMillis());
    }

    public void update() {
        LAST_UPDATE_TIME_UPDATER.set(this, Clock.currentTimeMillis());
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

}
