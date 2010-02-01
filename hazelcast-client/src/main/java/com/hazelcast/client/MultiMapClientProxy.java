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

package com.hazelcast.client;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.impl.ClusterOperation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.ProxyHelper.check;
import static com.hazelcast.client.Serializer.toByte;

public class MultiMapClientProxy<K, V> implements ClientProxy, MultiMap<K, V>, EntryHolder {
    private final String name;
    private final ProxyHelper proxyHelper;
    private final HazelcastClient client;

    public MultiMapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(name, client);
        this.client = client;
    }

    public void setOutRunnable(OutRunnable out) {
        proxyHelper.setOutRunnable(out);
    }

    public String getName() {
        return name.substring(4);
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        check(listener);
        if (client.listenerManager.entryListenerManager.noEntryListenerRegistered(key, name)) {
            Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, toByte(key), null);
            request.setLongValue(includeValue ? 1 : 0);
            Call c = proxyHelper.createCall(request);
            client.listenerManager.addListenerCall(c);
            proxyHelper.doCall(c);
        }
        client.listenerManager.entryListenerManager.registerEntryListener(name, key, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, null, null);
        client.listenerManager.entryListenerManager.removeEntryListener(name, null, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        check(key);
        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, key, null);
        client.listenerManager.entryListenerManager.removeEntryListener(name, key, listener);
    }

    public void lock(K key) {
        ProxyHelper.check(key);
        doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, -1, null);
    }

    public boolean tryLock(K key) {
        check(key);
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, 0, null);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        check(time, timeunit);
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, time, timeunit);
    }

    private Object doLock(ClusterOperation operation, Object key, long timeout, TimeUnit timeUnit) {
        Packet request = proxyHelper.prepareRequest(operation, key, timeUnit);
        request.setTimeout(timeout);
        Packet response = proxyHelper.callAndGetResult(request);
        return proxyHelper.getValue(response);
    }

    public void unlock(K key) {
        check(key);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_UNLOCK, key, null);
    }

    public boolean put(K key, V value) {
        check(key);
        check(value);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_MULTI, key, value);
    }

    public Collection get(Object key) {
        check(key);
        return (Collection) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_GET, key, null);
    }

    public boolean remove(Object key, Object value) {
        check(key);
        check(value);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_MULTI, key, value);
    }

    public Collection remove(Object key) {
        check(key);
        return (Collection) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_MULTI, key, null);
    }

    public Set keySet() {
        final Collection<Object> collection = proxyHelper.keys(null);
        LightKeySet<Object> set = new LightKeySet<Object>(this, new HashSet<Object>(collection));
        return set;
    }

    public Collection values() {
        Set<Map.Entry> set = entrySet();
        return new ValueCollection(this, set);
    }

    public Set entrySet() {
        Set<Object> keySet = keySet();
        return new LightMultiMapEntrySet<Object, Collection>(keySet, this, getInstanceType());
    }

    public boolean containsKey(Object key) {
        check(key);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, key, null);
    }

    public boolean containsValue(Object value) {
        check(value);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, null, value);
    }

    public boolean containsEntry(Object key, Object value) {
        check(key);
        check(value);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, key, value);
    }

    public int size() {
        return (Integer) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public int valueCount(Object key) {
        check(key);
        return (Integer) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_VALUE_COUNT, key, null);
    }

    public InstanceType getInstanceType() {
        return InstanceType.MULTIMAP;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiMap && o != null) {
            return getName().equals(((MultiMap) o).getName());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }
}
