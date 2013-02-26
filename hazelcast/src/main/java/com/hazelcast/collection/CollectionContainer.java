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

import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockStoreView;
import com.hazelcast.concurrent.lock.SharedLockService;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ali 1/2/13
 */
public class CollectionContainer {

    final CollectionProxyId proxyId;

    final CollectionService service;

    final NodeEngine nodeEngine;

    final MultiMapConfig config;

    final ConcurrentMap<Data, Collection<CollectionRecord>> collections = new ConcurrentHashMap<Data, Collection<CollectionRecord>>(1000);

    final LockNamespace lockNamespace;

    final LockStoreView lockStore;

    final int partitionId;

    final AtomicLong idGen = new AtomicLong();

    public CollectionContainer(CollectionProxyId proxyId, CollectionService service, int partitionId) {
        this.proxyId = proxyId;
        this.service = service;
        this.nodeEngine = service.getNodeEngine();
        this.partitionId = partitionId;
        this.config = new MultiMapConfig(nodeEngine.getConfig().getMultiMapConfig(proxyId.name));

        this.lockNamespace = new LockNamespace(CollectionService.SERVICE_NAME, proxyId);
        final SharedLockService lockService = nodeEngine.getSharedService(SharedLockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null :
                lockService.createLockStore(partitionId, lockNamespace, config.getSyncBackupCount(), config.getAsyncBackupCount());
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public long nextId() {
        return idGen.getAndIncrement();
    }

    public Collection<CollectionRecord> getOrCreateCollection(Data dataKey) {
        Collection<CollectionRecord> coll = collections.get(dataKey);
        if (coll == null) {
            coll = service.createNew(proxyId);
            collections.put(dataKey, coll);
        }
        return coll;
    }

    public Collection<CollectionRecord> getCollection(Data dataKey) {
        return collections.get(dataKey);
    }

    public Collection<CollectionRecord> removeCollection(Data dataKey) {
        return collections.remove(dataKey);
    }

    public Set<Data> keySet() {
        Set<Data> keySet = collections.keySet();
        Set<Data> keys = new HashSet<Data>(keySet.size());
        keys.addAll(keySet);
        return keys;
    }

    public Collection<CollectionRecord> values() {
        Collection<CollectionRecord> valueCollection = new LinkedList<CollectionRecord>();
        for (Collection<CollectionRecord> coll : collections.values()) {
            valueCollection.addAll(coll);
        }
        return valueCollection;
    }

    public boolean containsKey(Data key) {
        return collections.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        Collection<CollectionRecord> coll = collections.get(key);
        if (coll == null) {
            return false;
        }
        CollectionRecord record = new CollectionRecord(binary ? value : nodeEngine.toObject(value));
        return coll.contains(record);
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
        for (Map.Entry<Data, Collection<CollectionRecord>> entry : collections.entrySet()) {
            Data key = entry.getKey();
            Collection<CollectionRecord> col = copyCollection(entry.getValue());
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
        for (Collection<CollectionRecord> coll : collections.values()) {
            size += coll.size();
        }
        return size;
    }

    public void clearCollections() {
        final Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        Map<Data, Collection<CollectionRecord>> temp = new HashMap<Data, Collection<CollectionRecord>>(locks.size());
        for (Data key : locks) {
            temp.put(key, collections.get(key));
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

    public ConcurrentMap<Data, Collection<CollectionRecord>> getCollections() {
        return collections; //TODO for testing only
    }

    public void destroy() {
        final SharedLockService lockService = nodeEngine.getSharedService(SharedLockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.destroyLockStore(partitionId, lockNamespace);
        }
        collections.clear();
    }

}
