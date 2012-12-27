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

package com.hazelcast.client;

import com.hazelcast.client.impl.EntryListenerManager;
import com.hazelcast.client.util.EntryHolder;
import com.hazelcast.client.util.LightMultiMapEntrySet;
import com.hazelcast.client.util.ValueCollection;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Prefix;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.PacketProxyHelper.check;
import static com.hazelcast.client.PacketProxyHelper.checkTime;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class MultiMapClientProxy<K, V> implements MultiMap<K, V>, EntryHolder {
    private final String name;
    //    private final PacketProxyHelper proxyHelper;
    private final ProtocolProxyHelper protocolProxyHelper;
    private final HazelcastClient client;

    public MultiMapClientProxy(HazelcastClient client, String name) {
        this.name = name;
//        this.proxyHelper = new PacketProxyHelper(name, client);
        protocolProxyHelper = new ProtocolProxyHelper(name, client);
        this.client = client;
    }

    public String getName() {
        return name.substring(Prefix.MULTIMAP.length());
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        check(listener);
        Boolean noEntryListenerRegistered = entryListenerManager().noListenerRegistered(key, name, includeValue);
        if (noEntryListenerRegistered == null) {
//            proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, key, null);
            if (key == null)
                protocolProxyHelper.doCommand(Command.MMREMOVELISTENER, getName(), null);
            else
                protocolProxyHelper.doCommand(Command.MMREMOVELISTENER, getName(), toData(key));
            noEntryListenerRegistered = Boolean.TRUE;
        }
        if (noEntryListenerRegistered.booleanValue()) {
//            Call c = entryListenerManager().createNewAddListenerCall(proxyHelper, key, includeValue);
//            proxyHelper.doCall(c);
//            if(key==null)
//            protocolProxyHelper.doCommand(Command.MMADDLISTENER, )
        }
        entryListenerManager().registerListener(name, key, includeValue, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
//        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, null, null);
        entryListenerManager().removeListener(name, null, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        check(key);
//        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, key, null);
        entryListenerManager().removeListener(name, key, listener);
    }

    private EntryListenerManager entryListenerManager() {
        return client.getListenerManager().getEntryListenerManager();
    }

    public void lock(K key) {
        check(key);
        protocolProxyHelper.doCommand(Command.MMLOCK, getName(), toData(key));
    }

    public boolean tryLock(K key) {
        check(key);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMTRYLOCK, new String[]{getName()}, toData(key));
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMTRYLOCK, new String[]{getName(),
                String.valueOf(timeunit.toMillis(time))}, toData(key));
    }

    public void unlock(K key) {
        check(key);
        protocolProxyHelper.doCommand(Command.MMUNLOCK, getName(), toData(key));
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        checkTime(time, timeunit);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMLOCK, new String[]{getName(),
                String.valueOf(timeunit.toMillis(time))}, null);
    }

    public void unlockMap() {
        protocolProxyHelper.doCommand(Command.MMUNLOCK, getName(), null);
    }

    public LocalMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException();
    }

    public boolean put(K key, V value) {
        check(key);
        check(value);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMPUT, new String[]{getName()}, toData(key), toData(value));
    }

    public Collection get(Object key) {
        check(key);
        return protocolProxyHelper.doCommandAsList(Command.MMGET, new String[]{getName()}, toData(key));
    }

    public boolean remove(Object key, Object value) {
        check(key);
        check(value);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMREMOVE, new String[]{getName()}, toData(key), toData(value));
    }

    public Collection remove(Object key) {
        check(key);
        return protocolProxyHelper.doCommandAsList(Command.MMREMOVE, new String[]{getName()}, toData(key));
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException();
    }

    public Set keySet() {
        Protocol protocol = protocolProxyHelper.doCommand(Command.MMKEYS, new String[]{getName()}, null);
        Set set = new HashSet();
        if (protocol.hasBuffer()) {
            for (ByteBuffer bb : protocol.buffers) {
                set.add(toObject(new Data(bb.array())));
            }
        }
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
        return protocolProxyHelper.doCommandAsBoolean(Command.MMCONTAINSKEY, new String[]{getName()}, toData(key));
    }

    public boolean containsValue(Object value) {
        check(value);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMCONTAINSVALUE, new String[]{getName()}, toData(value));
    }

    public boolean containsEntry(Object key, Object value) {
        check(key);
        check(value);
        return protocolProxyHelper.doCommandAsBoolean(Command.MMCONTAINSENTRY, new String[]{getName()}, toData(key), toData(value));
    }

    public int size() {
        return protocolProxyHelper.doCommandAsInt(Command.MMSIZE, new String[]{getName()}, null);
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public int valueCount(Object key) {
        check(key);
        return protocolProxyHelper.doCommandAsInt(Command.MMVALUECOUNT, new String[]{getName()}, toData(key));
    }

    public InstanceType getInstanceType() {
        return InstanceType.MULTIMAP;
    }

    public void destroy() {
//        proxyHelper.destroy();
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
