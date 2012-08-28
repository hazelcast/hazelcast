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

package com.hazelcast.client;

import com.hazelcast.client.impl.EntryListenerManager;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.Keys;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.client.PacketProxyHelper.check;
import static com.hazelcast.client.PacketProxyHelper.checkTime;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class MapClientProxy<K, V> implements IMap<K, V>, EntryHolder {
    final ProtocolProxyHelper protocolProxyHelper;
    final private String name;

    public MapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.protocolProxyHelper = new ProtocolProxyHelper(getName(), client);
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        check(listener);
        Boolean noEntryListenerRegistered = listenerManager().noListenerRegistered(key, getName(), includeValue);
        Data dKey = toData(key);
        Data[] datas = dKey == null ? new Data[]{} : new Data[]{dKey};
        if (noEntryListenerRegistered == null) {
            protocolProxyHelper.doCommand(Command.MREMOVELISTENER, getName(), datas);
            noEntryListenerRegistered = Boolean.TRUE;
        }
        if (noEntryListenerRegistered) {
            protocolProxyHelper.doCommand(Command.MADDLISTENER, new String[]{getName(), String.valueOf(includeValue)}, datas);
        }
        listenerManager().registerListener(getName(), key, includeValue, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
        protocolProxyHelper.doCommand(Command.MREMOVELISTENER, getName(), null);
        listenerManager().removeListener(getName(), getName(), listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        check(key);
        protocolProxyHelper.doCommand(Command.MREMOVELISTENER, getName(), toData(key));
        listenerManager().removeListener(getName(), key, listener);
    }

    private EntryListenerManager listenerManager() {
        return protocolProxyHelper.client.getListenerManager().getEntryListenerManager();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.entrySet();
    }

    private Map<K, V> getCopyOfTheMap(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = protocolProxyHelper.doCommand(Command.MENTRYSET, new String[]{getName()}, null);
        else
            protocol = protocolProxyHelper.doCommand(Command.MENTRYSET, new String[]{getName()}, toData(predicate));
        int size = protocol.buffers == null ? 0 : protocol.buffers.length;
        Map<K, V> map = new HashMap<K, V>();
        for (int i = 0; i < size; ) {
            K key = (K) toObject(new Data(protocol.buffers[i++].array()));
            V value = (V) toObject(new Data(protocol.buffers[i++].array()));
            map.put(key, value);
        }
        return map;
    }

    public void flush() {
        protocolProxyHelper.doCommand(Command.MFLUSH, getName(), null);
    }

    public boolean evict(Object key) {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MEVICT, new String[]{getName()}, toData(key));
        Boolean evicted = Boolean.valueOf(protocol.args[0]);
        return evicted;
    }

    public MapEntry<K, V> getMapEntry(K key) {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MGETENTRY, new String[]{getName()}, toData(key));
        if (!protocol.hasBuffer()) {
            return null;
        }
        ClientMapEntry<K, V> clientMapEntry = new ClientMapEntry<K, V>(key, this);
        clientMapEntry.setCost(Long.valueOf(protocol.args[0]));
        clientMapEntry.setCreationTime(Long.valueOf(protocol.args[1]));
        clientMapEntry.setExpirationTime(Long.valueOf(protocol.args[2]));
        clientMapEntry.setHits(Integer.valueOf(protocol.args[3]));
        clientMapEntry.setLastAccessTime(Long.valueOf(protocol.args[4]));
        clientMapEntry.setLastStoredTime(Long.valueOf(protocol.args[5]));
        clientMapEntry.setLastUpdateTime(Long.valueOf(protocol.args[6]));
        clientMapEntry.setVersion(Long.valueOf(protocol.args[7]));
        clientMapEntry.setValid(Boolean.valueOf(protocol.args[8]));
        clientMapEntry.setV((V) toObject(new Data(protocol.buffers[0].array())));
        return clientMapEntry;
    }

    public Set<K> keySet(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = protocolProxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()}, null);
        else
            protocol = protocolProxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()}, toData(predicate));
        if (!protocol.hasBuffer()) return Collections.emptySet();
        Set<K> set = new HashSet<K>(protocol.buffers.length);
        for (ByteBuffer b : protocol.buffers) {
            set.add((K) toObject(new Data(b.array())));
        }
        return set;
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        checkTime(time, timeunit);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MLOCKMAP, new String[]{getName(),
                String.valueOf(timeunit.toMillis(time))}, null);
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlockMap() {
        protocolProxyHelper.doCommand(Command.MUNLOCKMAP, getName(), null);
    }

    public void lock(K key) {
        check(key);
        protocolProxyHelper.doCommand(Command.MLOCK, getName(), toData(key));
    }

    public boolean isLocked(K key) {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MISKEYLOCKED, new String[]{getName()}, toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(K key) {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MTRYLOCK, new String[]{getName(), "0"}, toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public V tryLockAndGet(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MTRYLOCKANDGET, new String[]{getName(), "" + timeunit.toMillis(timeout)}, toData(key));
        if (protocol.args != null && protocol.args.length > 0) {
            if ("timeout".equals(protocol.args[0])) {
                throw new TimeoutException();
            }
        }
        return protocol.hasBuffer() ? (V) toObject(new Data(protocol.buffers[0].array())) : null;
    }

    public void putAndUnlock(K key, V value) {
        check(key);
        check(value);
        protocolProxyHelper.doCommand(Command.MPUTANDUNLOCK, getName(), toData(key), toData(value));
//        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_AND_UNLOCK, key, value);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MTRYLOCK, new String[]{getName(), "" + timeunit.toMillis(time)}, toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlock(K key) {
        check(key);
        protocolProxyHelper.doCommand(Command.MUNLOCK, getName(), toData(key));
    }

    public void forceUnlock(K key) {
        check(key);
        protocolProxyHelper.doCommand(Command.MFORCEUNLOCK, getName(), toData(key));
    }

    public Collection<V> values(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.values();
//        return new ValueCollection<K, V>(this, set);
    }

    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MPUTIFABSENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, toData(key), toData(value));
    }

    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public boolean remove(Object arg0, Object arg1) {
        check(arg0);
        check(arg1);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MREMOVEIFSAME, new String[]{getName()}, toData(arg0), toData(arg1));
        return Boolean.valueOf(protocol.args[0]);
    }

    public V replace(K arg0, V arg1) {
        check(arg0);
        check(arg1);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MREPLACEIFNOTNULL, getName(), toData(arg0), toData(arg1));
    }

    public boolean replace(K arg0, V arg1, V arg2) {
        check(arg0);
        check(arg1);
        check(arg2);
        Keys keys = new Keys();
        keys.getKeys().add(toData(arg1));
        keys.getKeys().add(toData(arg2));
        Protocol protocol = protocolProxyHelper.doCommand(Command.MREPLACEIFSAME, new String[]{getName()}, toData(arg0), toData(arg1), toData(arg2));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public boolean containsKey(Object arg0) {
        check(arg0);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MCONTAINSKEY, new String[]{getName()}, toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean containsValue(Object arg0) {
        check(arg0);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MCONTAINSVALUE, new String[]{getName()}, toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return entrySet(null);
    }

    public V get(Object key) {
        check(key);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MGET, getName(), toData((K) key));
    }

    public Map<K, V> getAll(Set<K> setKeys) {
        check(setKeys);
        List<Data> dataList = new ArrayList<Data>(setKeys.size());
        for (K key : setKeys) {
            dataList.add(toData(key));
        }
        Map<K, V> map = new HashMap<K, V>(setKeys.size());
        Protocol protocol = protocolProxyHelper.doCommand(Command.MGETALL, new String[]{getName()}, dataList.toArray(new Data[]{}));
        if (protocol.hasBuffer()) {
            int i = 0;
            System.out.println("Get all and buffer length is " + protocol.buffers.length);
            while (i < protocol.buffers.length) {

                K key = protocol.buffers[i].array().length == 0 ? null : (K) toObject(new Data(protocol.buffers[i].array()));
                i++;
                V value = protocol.buffers[i].array().length == 0 ? null : (V) toObject(new Data(protocol.buffers[i].array()));
                i++;
                if(value!=null){
                    map.put(key, value);
                }
            }
        }
        return map;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException();
    }

    public Set<K> localKeySet(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    public LocalMapStats getLocalMapStats() {
        throw new UnsupportedOperationException();
    }

    public Set<K> keySet() {
        return keySet(null);
    }

    public Future<V> getAsync(K key) {
        check(key);
        return protocolProxyHelper.doAsync(Command.MGET, new String[]{getName()}, toData(key));
    }

    public Future<V> putAsync(K key, V value) {
        check(key);
        check(value);
        return protocolProxyHelper.doAsync(Command.MPUT, new String[]{getName()}, toData(key), toData(value));
    }

    public Future<V> removeAsync(K key) {
        check(key);
        return protocolProxyHelper.doAsync(Command.MREMOVE, new String[]{getName()}, toData(key));
    }

    public V put(K key, V value) {
        check(key);
        check(value);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MPUT, getName(), toData(key), toData(value));
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MPUT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, toData(key), toData(value));
    }

    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        protocolProxyHelper.doCommand(Command.MSET, new String[]{getName(), "" + timeunit.toMillis(ttl)}, toData(key), toData(value));
    }

    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        protocolProxyHelper.doCommand(Command.MPUTTRANSIENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, toData(key), toData(value));
    }

    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        check(key);
        check(value);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MTRYPUT, new String[]{getName(), "" + timeunit.toMillis(timeout)}, toData(key), toData(value));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void putAll(final Map<? extends K, ? extends V> map) {
        Pairs pairs = new Pairs(map.size());
        for (final K key : map.keySet()) {
            final V value = map.get(key);
            pairs.addKeyValue(new KeyValue(toData(key), toData(value)));
        }
        List<Data> dataList = new ArrayList<Data>();
        for (K k : map.keySet()) {
            if (k != null) {
                V v = map.get(k);
                if (v != null) {
                    dataList.add(toData(k));
                    dataList.add(toData(v));
                }
            }
        }
        protocolProxyHelper.doCommand(Command.MPUTALL, getName(), dataList.toArray(new Data[]{}));
    }

    public V remove(Object arg0) {
        check(arg0);
        return (V) protocolProxyHelper.doCommandAsObject(Command.MREMOVE, getName(), toData(arg0));
    }

    public Object tryRemove(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Protocol protocol = protocolProxyHelper.doCommand(Command.MTRYREMOVE, new String[]{getName(), "" + timeunit.toMillis(timeout)}, toData(key));
        if (protocol.args != null && protocol.args.length > 0) {
            if ("timeout".equals(protocol.args[0])) {
                throw new TimeoutException();
            }
        }
        return protocol.hasBuffer() ? (V) toObject(new Data(protocol.buffers[0].array())) : null;
    }

    public int size() {
        Protocol protocol = protocolProxyHelper.doCommand(Command.MSIZE, new String[]{getName()}, null);
        return Integer.valueOf(protocol.args[0]);
    }

    public Collection<V> values() {
        return values(null);
    }

    public Object getId() {
        return name;
    }

    public InstanceType getInstanceType() {
        return InstanceType.MAP;
    }

    public void addIndex(String attribute, boolean ordered) {
        protocolProxyHelper.doCommand(Command.MADDINDEX, new String[]{getName(), attribute, String.valueOf(ordered)}, null);
    }

    public void addIndex(Expression<?> expression, boolean ordered) {
        protocolProxyHelper.doCommand(Command.MADDINDEX, new String[]{getName(), String.valueOf(ordered)}, toData(expression));
    }

    public String getName() {
        return name.substring(Prefix.MAP.length());
    }

    public void destroy() {
        protocolProxyHelper.doCommand(Command.DESTROY, new String[]{InstanceType.MAP.name(), getName()}, null);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IMap) {
            return getName().equals(((IMap) o).getName());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }
}
