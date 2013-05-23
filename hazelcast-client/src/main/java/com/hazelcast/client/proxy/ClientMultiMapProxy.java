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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.client.*;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @ali 5/19/13
 */
public class ClientMultiMapProxy<K, V> extends ClientProxy implements MultiMap<K, V> {

    final CollectionProxyId proxyId;

    public ClientMultiMapProxy(String serviceName, CollectionProxyId objectId) {
        super(serviceName, objectId);
        proxyId = objectId;
    }

    public boolean put(K key, V value) {
        Data keyData = getSerializationService().toData(key);
        Data valueData = getSerializationService().toData(value);
        PutRequest request = new PutRequest(proxyId, keyData, valueData, -1, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public Collection<V> get(K key) {
        Data keyData = getSerializationService().toData(key);
        GetAllRequest request = new GetAllRequest(proxyId, keyData);
        PortableCollection result = invoke(request, keyData);
        return toObjectCollection(result, true);
    }

    public boolean remove(Object key, Object value) {
        Data keyData = getSerializationService().toData(key);
        Data valueData = getSerializationService().toData(value);
        RemoveRequest request = new RemoveRequest(proxyId, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public Collection<V> remove(Object key) {
        Data keyData = getSerializationService().toData(key);
        RemoveAllRequest request = new RemoveAllRequest(proxyId, keyData, ThreadUtil.getThreadId());
        PortableCollection result = invoke(request, keyData);
        return toObjectCollection(result, true);
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public Set<K> keySet() {
        KeySetRequest request = new KeySetRequest(proxyId);
        PortableCollection result = invoke(request);
        return (Set)toObjectCollection(result, false);
    }

    public Collection<V> values() {
        ValuesRequest request = new ValuesRequest(proxyId);
        PortableCollection result = invoke(request);
        return toObjectCollection(result, true);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        EntrySetRequest request = new EntrySetRequest(proxyId);
        PortableEntrySetResponse result = invoke(request);
        Set<Map.Entry> dataEntrySet = result.getEntrySet();
        Set<Map.Entry<K,V>> entrySet = new HashSet<Map.Entry<K, V>>(dataEntrySet.size());
        for (Map.Entry entry : dataEntrySet) {
            Object key = getSerializationService().toObject((Data)entry.getKey());
            Object val = getSerializationService().toObject((Data)entry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry(key, val));
        }
        return entrySet;
    }

    public boolean containsKey(K key) {
        Data keyData = getSerializationService().toData(key);
        ContainsEntryRequest request = new ContainsEntryRequest(proxyId, keyData, null);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean containsValue(Object value) {
        Data valueData = getSerializationService().toData(value);
        ContainsEntryRequest request = new ContainsEntryRequest(proxyId, null, valueData);
        Boolean result = invoke(request);
        return result;
    }

    public boolean containsEntry(K key, V value) {
        Data keyData = getSerializationService().toData(key);
        Data valueData = getSerializationService().toData(value);
        ContainsEntryRequest request = new ContainsEntryRequest(proxyId, keyData, valueData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public int size() {
        SizeRequest request = new SizeRequest(proxyId);
        Integer result = invoke(request);
        return result;
    }

    public void clear() {
        ClearRequest request = new ClearRequest(proxyId);
        invoke(request);
    }

    public int valueCount(K key) {
        Data keyData = getSerializationService().toData(key);
        CountRequest request = new CountRequest(proxyId, keyData);
        Integer result = invoke(request, keyData);
        return result;
    }

    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        AddEntryListenerRequest request = new AddEntryListenerRequest(proxyId, null, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    public boolean removeEntryListener(String registrationId) {
        return stopListening(registrationId);
    }

    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final Data keyData = getSerializationService().toData(key);
        AddEntryListenerRequest request = new AddEntryListenerRequest(proxyId, keyData, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    public void lock(K key) {
        final Data keyData = getSerializationService().toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(), proxyId);
        invoke(request, keyData);
    }

    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        final Data keyData = getSerializationService().toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(),
                getTimeInMillis(leaseTime, timeUnit), -1, proxyId);
        invoke(request, keyData);
    }

    public boolean isLocked(K key) {
        final Data keyData = getSerializationService().toData(key);
        final MultiMapIsLockedRequest request = new MultiMapIsLockedRequest(keyData, proxyId);
        final Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final Data keyData = getSerializationService().toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(),
                Long.MAX_VALUE, getTimeInMillis(time, timeunit), proxyId);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public void unlock(K key) {
        final Data keyData = getSerializationService().toData(key);
        MultiMapUnlockRequest request = new MultiMapUnlockRequest(keyData, ThreadUtil.getThreadId(), proxyId);
        invoke(request, keyData);
    }

    public void forceUnlock(K key) {
        final Data keyData = getSerializationService().toData(key);
        MultiMapUnlockRequest request = new MultiMapUnlockRequest(keyData, ThreadUtil.getThreadId(), true, proxyId);
        invoke(request, keyData);
    }

    public LocalMultiMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    protected void onDestroy() {
    }

    public String getName() {
        return proxyId.getName();
    }

    private <T> T invoke(Object req, Data key) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, key);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnRandomTarget(req);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Collection toObjectCollection(PortableCollection result, boolean list) {
        Collection<Data> coll = result.getCollection();
        Collection collection;
        if (list){
            collection = new ArrayList(coll == null ? 0 : coll.size());
        } else {
            collection = new HashSet(coll == null ? 0 : coll.size());
        }
        if (coll == null){
            return collection;
        }
        for (Data data : coll) {
            collection.add(getSerializationService().toObject(data));
        }
        return collection;
    }

    private SerializationService getSerializationService(){
        return getContext().getSerializationService();
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<PortableEntryEvent> createHandler(final EntryListener<K,V> listener, final boolean includeValue){
        return new EventHandler<PortableEntryEvent>() {
            public void handle(PortableEntryEvent event) {
                V value = null;
                V oldValue = null;
                if (includeValue){
                    value = (V)getSerializationService().toObject(event.getValue());
                    oldValue = (V)getSerializationService().toObject(event.getOldValue());
                }
                K key = (K)getSerializationService().toObject(event.getKey());
                Member member = getContext().getClusterService().getMember(event.getUuid());
                EntryEvent<K,V> entryEvent = new EntryEvent<K, V>(proxyId.getName(), member,
                        event.getEventType().getType(), key, oldValue, value);
                switch (event.getEventType()){
                    case ADDED:
                        listener.entryAdded(entryEvent);
                    case REMOVED:
                        listener.entryRemoved(entryEvent);
                    case UPDATED:
                        listener.entryUpdated(entryEvent);
                    case EVICTED:
                        listener.entryEvicted(entryEvent);
                }
            }
        };
    }
}
