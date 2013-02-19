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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.EntryListenerManager;
import com.hazelcast.client.proxy.listener.EntryEventLRH;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.util.EntryHolder;
import com.hazelcast.client.util.LightMultiMapEntrySet;
import com.hazelcast.client.util.ValueCollection;
import com.hazelcast.collection.multimap.ObjectMultiMapProxy;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.proxy.ProxyHelper.check;
import static com.hazelcast.client.proxy.ProxyHelper.checkTime;
import static java.lang.String.valueOf;

public class MultiMapClientProxy<K, V> implements MultiMap<K, V>, EntryHolder<K,V> {
    private final String name;
    private final ProxyHelper proxyHelper;
    private final HazelcastClient client;
    final Map<EntryListener<K, V>, Map<Object, ListenerThread>> listenerMap = new IdentityHashMap<EntryListener<K, V>, Map<Object, ListenerThread>>();

    public MultiMapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
        this.client = client;
    }

    public String getName() {
        return name;
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }



    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(final EntryListener<K, V> listener, final K key, final boolean includeValue) {
        Data dKey = key == null ? null : proxyHelper.toData(key);
        Protocol request = proxyHelper.createProtocol(Command.MMLISTEN, new String[]{name, valueOf(includeValue)}, new Data[]{dKey});
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.multiMapListener.",
                client, request, new EntryEventLRH<K, V>(listener, key, includeValue, this));
        storeListener(listener, key, thread);
        thread.start();
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
        removeEntryListener(listener, null);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        Map<Object, ListenerThread> map = listenerMap.remove(listener);
        if (map != null) {
            ListenerThread thread = map.remove(key);
            if (thread != null) thread.stopListening();
        }
    }

    private void storeListener(EntryListener<K, V> listener, K key, ListenerThread thread) {
        synchronized (listenerMap) {
            Map<Object, ListenerThread> map = listenerMap.get(listener);
            if (map == null) {
                map = new HashMap<Object, ListenerThread>();
                listenerMap.put(listener, map);
            }
            map.put(key, thread);
        }
    }

    public void lock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.lock(getName(), dKey, Command.MMLOCK, new String[]{getName()}, dKey);
    }

    public boolean tryLock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol response = proxyHelper.lock(getName(), dKey, Command.MMTRYLOCK, new String[]{getName()}, dKey);
        return Boolean.valueOf(response.args[0]);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        String[] args = new String[]{getName(), String.valueOf(timeunit.toMillis(time))};
        Protocol response = proxyHelper.lock(getName(), dKey, Command.MMTRYLOCK, args, dKey);
        return Boolean.valueOf(response.args[0]);
    }

    public void unlock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.unlock(getName(), dKey, Command.MMUNLOCK, new String[]{getName()}, dKey);
    }

    public LocalMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException();
    }

    public boolean put(K key, V value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        Data[] datas = new Data[]{dKey, proxyHelper.toData(value)};
        return proxyHelper.doCommandAsBoolean(dKey, Command.MMPUT, new String[]{getName()}, datas);
    }

    public Collection get(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return proxyHelper.doCommandAsList(dKey, Command.MMGET, new String[]{getName()}, dKey);
    }

    public boolean remove(Object key, Object value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        Data[] datas = new Data[]{dKey, proxyHelper.toData(value)};
        return proxyHelper.doCommandAsBoolean(dKey, Command.MMREMOVE, new String[]{getName()}, datas);
    }

    public Collection remove(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return proxyHelper.doCommandAsList(dKey, Command.MMREMOVE, new String[]{getName()}, dKey);
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException();
    }

    public Set keySet() {
        List<Data> list =  proxyHelper.doCommandAsList(Command.MMKEYS, new String[]{getName()});
        return new HashSet(list);
    }

    public Collection values() {
        Set<Map.Entry> set = entrySet();
        return new ValueCollection(this, set);
    }

    public Set entrySet() {
        Set<Object> keySet = keySet();
        return new LightMultiMapEntrySet<Object, Collection>(keySet, this);
    }

    public boolean containsKey(Object key) {
        check(key);
        String[] args = new String[]{getName()};
        Data dKey = proxyHelper.toData(key);
        return proxyHelper.doCommandAsBoolean(dKey, Command.MMCONTAINSKEY, args, dKey);
    }

    public boolean containsValue(Object value) {
        check(value);
        String[] args = new String[]{getName()};
        return proxyHelper.doCommandAsBoolean(Command.MMCONTAINSVALUE, args, proxyHelper.toData(value));
    }

    public boolean containsEntry(Object key, Object value) {
        check(key);
        check(value);
        String[] args = new String[]{getName()};
        Data dKey = proxyHelper.toData(key);
        Data[] datas = new Data[]{dKey, proxyHelper.toData(value)};
        return proxyHelper.doCommandAsBoolean(dKey, Command.MMCONTAINSENTRY, args, datas);
    }

    public int size() {
        return proxyHelper.doCommandAsInt(Command.MMSIZE, new String[]{getName()});
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public int valueCount(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return proxyHelper.doCommandAsInt(dKey, Command.MMVALUECOUNT, new String[]{getName()}, dKey);
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{"multimap", getName()});
    }

    public Object getId() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiMap) {
            return getName().equals(((MultiMap) o).getName());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }
}
