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

package com.hazelcast.collection;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ali 1/2/13
 */
public class CollectionContainer {

    final CollectionProxyId proxyId;

    final CollectionService service;

    final NodeEngine nodeEngine;

    final MultiMapConfig config;

    final ConcurrentMap<Data, CollectionWrapper> collections = new ConcurrentHashMap<Data, CollectionWrapper>(1000);

    final DefaultObjectNamespace lockNamespace;

    final LockStore lockStore;

    final int partitionId;

    final AtomicLong idGen = new AtomicLong();
    final AtomicLong lastAccessTime = new AtomicLong();
    final AtomicLong lastUpdateTime = new AtomicLong();
    final long creationTime;

    public CollectionContainer(CollectionProxyId proxyId, CollectionService service, int partitionId) {
        this.proxyId = proxyId;
        this.service = service;
        this.nodeEngine = service.getNodeEngine();
        this.partitionId = partitionId;
        this.config = nodeEngine.getConfig().getMultiMapConfig(proxyId.name);

        this.lockNamespace = new DefaultObjectNamespace(CollectionService.SERVICE_NAME, proxyId);
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null : lockService.createLockStore(partitionId, lockNamespace);
        creationTime = Clock.currentTimeMillis();
    }

    public boolean canAcquireLock(Data dataKey, String caller, int threadId) {
        return lockStore != null && lockStore.canAcquireLock(dataKey, caller, threadId);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean txnLock(Data key, String caller, int threadId, long ttl){
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, int threadId){
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    public boolean forceUnlock(Data key){
        return lockStore != null && lockStore.forceUnlock(key);
    }

    public boolean extendLock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public String getLockOwnerInfo(Data dataKey) {
        return lockStore != null ? lockStore.getOwnerInfo(dataKey) : null;
    }

    public long nextId() {
        return idGen.getAndIncrement();
    }

    public CollectionWrapper getOrCreateCollectionWrapper(Data dataKey) {
        CollectionWrapper wrapper = collections.get(dataKey);
        if (wrapper == null) {
            Collection<CollectionRecord> coll = service.createNew(proxyId);
            wrapper = new CollectionWrapper(coll);
            collections.put(dataKey, wrapper);
        }
        return wrapper;
    }

    public CollectionWrapper getCollectionWrapper(Data dataKey) {
        return collections.get(dataKey);
    }

    public Collection<CollectionRecord> removeCollection(Data dataKey) {
        CollectionWrapper wrapper = collections.remove(dataKey);
        return wrapper != null ? wrapper.getCollection() : null;
    }

    public Set<Data> keySet() {
        Set<Data> keySet = collections.keySet();
        Set<Data> keys = new HashSet<Data>(keySet.size());
        keys.addAll(keySet);
        return keys;
    }

    public Collection<CollectionRecord> values() {
        Collection<CollectionRecord> valueCollection = new LinkedList<CollectionRecord>();
        for (CollectionWrapper wrapper : collections.values()) {
            valueCollection.addAll(wrapper.getCollection());
        }
        return valueCollection;
    }

    public boolean containsKey(Data key) {
        return collections.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        CollectionWrapper wrapper = collections.get(key);
        if (wrapper == null) {
            return false;
        }
        CollectionRecord record = new CollectionRecord(binary ? value : nodeEngine.toObject(value));
        return wrapper.getCollection().contains(record);
    }

    public boolean containsValue(boolean binary, Data value) {
        for (Data key : collections.keySet()) {
            if (containsEntry(binary, key, value)) {
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection<CollectionRecord>> copyCollections() {
        Map<Data, Collection<CollectionRecord>> map = new HashMap<Data, Collection<CollectionRecord>>(collections.size());
        for (Map.Entry<Data, CollectionWrapper> entry : collections.entrySet()) {
            Data key = entry.getKey();
            Collection<CollectionRecord> col = copyCollection(entry.getValue().getCollection());
            map.put(key, col);
        }
        return map;
    }

    private Collection<CollectionRecord> copyCollection(Collection<CollectionRecord> coll) {
        Collection<CollectionRecord> copy = new ArrayList<CollectionRecord>(coll.size());
        copy.addAll(coll);
        return copy;
    }

    public int size() {
        int size = 0;
        for (CollectionWrapper wrapper : collections.values()) {
            size += wrapper.getCollection().size();
        }
        return size;
    }

    public void clearCollections() {
        final Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        Map<Data, CollectionWrapper> temp = new HashMap<Data, CollectionWrapper>(locks.size());
        for (Data key : locks) {
            CollectionWrapper wrapper = collections.get(key);
            if (wrapper != null){
                temp.put(key, wrapper);
            }
        }
        collections.clear();
        collections.putAll(temp);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public MultiMapConfig getConfig() {
        return config;
    }

    public void destroy() {
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionId, lockNamespace);
        }
        collections.clear();
    }

    public void access() {
        lastAccessTime.set(Clock.currentTimeMillis());
    }

    public void update() {
        lastUpdateTime.set(Clock.currentTimeMillis());
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public long getLastUpdateTime() {
        return lastUpdateTime.get();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLockedCount() {
        return lockStore.getLockedKeys().size();
    }
}
