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

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.map.LockInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/2/13
 */
public class CollectionContainer {

    final CollectionProxyId proxyId;

    final CollectionService service;

    final NodeEngine nodeEngine;

    final MultiMapConfig config;

    final ConcurrentMap<Data, Object> objects = new ConcurrentHashMap<Data, Object>(1000);

    final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>(100);

    public CollectionContainer(CollectionProxyId proxyId, CollectionService service) {
        this.proxyId = proxyId;
        this.service = service;
        this.nodeEngine = service.getNodeEngine();
        this.config = new MultiMapConfig(nodeEngine.getConfig().getMultiMapConfig(proxyId.name));
    }

    public LockInfo getOrCreateLock(Data key) {
        LockInfo lock = locks.get(key);
        if (lock == null) {
            lock = new LockInfo();
            locks.put(key, lock);
        }
        return lock;
    }

    public boolean lock(Data dataKey, Address caller, int threadId, long ttl) {
        LockInfo lock = getOrCreateLock(dataKey);
        return lock.lock(caller, threadId, ttl);
    }

    public boolean isLocked(Data dataKey) {
        LockInfo lock = locks.get(dataKey);
        if (lock == null)
            return false;
        return lock.isLocked();
    }

    public boolean canAcquireLock(Data key, int threadId, Address caller) {
        LockInfo lock = locks.get(key);
        return lock == null || lock.testLock(threadId, caller);
    }

    public boolean unlock(Data dataKey, Address caller, int threadId) {
        LockInfo lock = locks.get(dataKey);
        boolean result = false;
        if (lock == null)
            return result;
        if (lock.testLock(threadId, caller)) {
            if (lock.unlock(caller, threadId)) {
                result = true;
            }
        }
        if (!lock.isLocked()) {
            locks.remove(dataKey);
        }
        return result;
    }

    public <T> T getOrCreateObject(Data dataKey) {
        Object obj = objects.get(dataKey);
        if (obj == null) {
            return (T) putNewObject(dataKey);
        }
        return (T) obj;
    }

    public <T> T getObject(Data dataKey) {
        return (T) objects.get(dataKey);
    }

    public <T> T removeObject(Data dataKey) {
        return (T) objects.remove(dataKey);
    }

    public Object putNewObject(Data dataKey) {
        Object obj = service.createNew(proxyId);
        objects.put(dataKey, obj);
        return obj;
    }

    public Set<Data> keySet() {
        Set<Data> keySet = objects.keySet();
        Set<Data> keys = new HashSet<Data>(keySet.size());
        keys.addAll(keySet);
        return keys;
    }

    public Collection values() {
        List valueList = new LinkedList();
        for (Object obj : objects.values()) {
            valueList.addAll((Collection) obj);
        }
        return valueList;
    }

    public boolean containsKey(Data key) {
        return objects.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        Collection coll = (Collection) objects.get(key);
        if (coll == null) {
            return false;
        }
        return coll.contains(binary ? value : getNodeEngine().toObject(value));
    }

    public boolean containsValue(boolean binary, Data value) {
        for (Data key : objects.keySet()) {
            if (containsEntry(binary, key, value)) {
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection> copyObjects() {
        Map<Data, Collection> map = new HashMap<Data, Collection>(objects.size());
        for (Map.Entry<Data, Object> entry : objects.entrySet()) {
            Data key = entry.getKey();
            Collection col = copyCollection((Collection) entry.getValue());
            map.put(key, col);
        }
        return map;
    }

    private Collection copyCollection(Collection coll) {
        Collection copy = new ArrayList(coll.size());
        copy.addAll(coll);
        return copy;
    }

    public int size() {
        int size = 0;
        for (Object obj : objects.values()) {
            size += ((Collection) obj).size();
        }
        return size;
    }

    public void clearObjects() {
        Map<Data, Object> temp = new HashMap<Data, Object>(locks.size());
        for (Data key : locks.keySet()) {
            temp.put(key, objects.get(key));
        }
        objects.clear();
        objects.putAll(temp);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public Object getProxyId() {
        return proxyId;
    }

    public MultiMapConfig getConfig() {
        return config;
    }

    public ConcurrentMap<Data, Object> getObjects() {
        return objects; //TODO for testing only
    }

    public ConcurrentMap<Data, LockInfo> getLocks() {
        return locks;   //TODO for testing only
    }

    public void clear() {
        objects.clear();
        locks.clear();
    }

}
