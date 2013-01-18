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

package com.hazelcast.client;

import com.hazelcast.client.impl.EntryListenerManager;
import com.hazelcast.client.util.EntryHolder;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MapClientProxy<K, V> implements IMap<K, V>, EntryHolder {
    final ProxyHelper proxyHelper;
    final private String name;

    public MapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper("", client);
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addQueryListener(EntryListener<K, V> kvEntryListener, Predicate<K, V> kvPredicate, K key, boolean includeValue) {
        //TODO add query listener will be implemented
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        check(listener);
        Boolean noEntryListenerRegistered = listenerManager().noListenerRegistered(key, getName(), includeValue);
        Data dKey = proxyHelper.toData(key);
        Data[] datas = dKey == null ? new Data[]{} : new Data[]{dKey};
        if (noEntryListenerRegistered == null) {
            proxyHelper.doCommand(Command.MREMOVELISTENER, getName(), datas);
            noEntryListenerRegistered = Boolean.TRUE;
        }
        if (noEntryListenerRegistered) {
            proxyHelper.doCommand(Command.MADDLISTENER, new String[]{getName(), String.valueOf(includeValue)}, datas);
        }
        listenerManager().registerListener(getName(), key, includeValue, listener);
    }

    private void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    private void check(EventListener listener) {
        if (listener == null) {
            throw new NullPointerException("Listener can not be null");
        }
    }

    private void checkTime(long time, TimeUnit timeunit) {
        if (time < 0) {
            throw new IllegalArgumentException("Time can not be less than 0.");
        }
        if (timeunit == null) {
            throw new NullPointerException("TimeUnit can not be null.");
        }
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
        proxyHelper.doCommand(Command.MREMOVELISTENER, getName(), null);
        listenerManager().removeListener(getName(), getName(), listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        check(key);
        proxyHelper.doCommand(Command.MREMOVELISTENER, getName(), proxyHelper.toData(key));
        listenerManager().removeListener(getName(), key, listener);
    }

    private EntryListenerManager listenerManager() {
        return proxyHelper.client.getListenerManager().getEntryListenerManager();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.entrySet();
    }

    private Map<K, V> getCopyOfTheMap(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = proxyHelper.doCommand(Command.MENTRYSET, new String[]{getName()}, null);
        else
            protocol = proxyHelper.doCommand(Command.MENTRYSET, new String[]{getName()}, proxyHelper.toData(predicate));
        int size = protocol.buffers == null ? 0 : protocol.buffers.length;
        Map<K, V> map = new HashMap<K, V>();
        for (int i = 0; i < size; ) {
            K key = (K) proxyHelper.toObject(protocol.buffers[i++]);
            V value = (V) proxyHelper.toObject(protocol.buffers[i++]);
            map.put(key, value);
        }
        return map;
    }

    public void flush() {
        proxyHelper.doCommand(Command.MFLUSH, getName(), null);
    }

    public boolean evict(Object key) {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MEVICT, new String[]{getName()}, proxyHelper.toData(key));
        Boolean evicted = Boolean.valueOf(protocol.args[0]);
        return evicted;
    }

    public MapEntry<K, V> getMapEntry(final K key) {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MGETENTRY, new String[]{getName()}, proxyHelper.toData(key));
        if (!protocol.hasBuffer()) {
            return null;
        }
        final long cost = Long.valueOf(protocol.args[0]);
        final long creationTime = Long.valueOf(protocol.args[1]);
        final long expTime = Long.valueOf(protocol.args[2]);
        final int hits = Integer.valueOf(protocol.args[3]);
        final long lastAccessTime = Long.valueOf(protocol.args[4]);
        final long lastStoredTime = Long.valueOf(protocol.args[5]);
        final long lastUpdateTime = Long.valueOf(protocol.args[6]);
        final long version = Long.valueOf(protocol.args[7]);
        final boolean valid = Boolean.valueOf(protocol.args[7]);
        final V v = (V) proxyHelper.toObject(protocol.buffers[0]);
        return new MapEntry<K, V>() {
            public long getCost() {
                return cost;
            }

            public long getCreationTime() {
                return creationTime;
            }

            public long getExpirationTime() {
                return expTime;
            }

            public int getHits() {
                return hits;
            }

            public long getLastAccessTime() {
                return lastAccessTime;
            }

            public long getLastStoredTime() {
                return lastStoredTime;
            }

            public long getLastUpdateTime() {
                return lastUpdateTime;
            }

            public long getVersion() {
                return version;
            }

            public boolean isValid() {
                return valid;
            }

            public K getKey() {
                return key;
            }

            public V getValue() {
                return v;
            }

            public V setValue(V value) {
                return MapClientProxy.this.put(key, value);
            }
        };
    }

    public Set<K> keySet(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = proxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()}, null);
        else
            protocol = proxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()}, proxyHelper.toData(predicate));
        if (!protocol.hasBuffer()) return Collections.emptySet();
        Set<K> set = new HashSet<K>(protocol.buffers.length);
        for (Data b : protocol.buffers) {
            set.add((K) proxyHelper.toObject(b));
        }
        return set;
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        checkTime(time, timeunit);
        Protocol protocol = proxyHelper.doCommand(Command.MLOCKMAP, new String[]{getName(),
                String.valueOf(timeunit.toMillis(time))}, null);
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlockMap() {
        proxyHelper.doCommand(Command.MUNLOCKMAP, getName(), null);
    }

    public void lock(K key) {
        check(key);
        proxyHelper.doCommand(Command.MLOCK, getName(), proxyHelper.toData(key));
    }

    public boolean isLocked(K key) {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MISKEYLOCKED, new String[]{getName()}, proxyHelper.toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(K key) {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MTRYLOCK, new String[]{getName(), "0"}, proxyHelper.toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public V tryLockAndGet(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MTRYLOCKANDGET, new String[]{getName(), "" + timeunit.toMillis(timeout)}, proxyHelper.toData(key));
        if (protocol.args != null && protocol.args.length > 0) {
            if ("timeout".equals(protocol.args[0])) {
                throw new TimeoutException();
            }
        }
        return protocol.hasBuffer() ? (V) proxyHelper.toObject(protocol.buffers[0]) : null;
    }

    public void putAndUnlock(K key, V value) {
        check(key);
        check(value);
        proxyHelper.doCommand(Command.MPUTANDUNLOCK, getName(), proxyHelper.toData(key), proxyHelper.toData(value));
//        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_AND_UNLOCK, key, value);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MTRYLOCK, new String[]{getName(), "" + timeunit.toMillis(time)}, proxyHelper.toData(key));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlock(K key) {
        check(key);
        proxyHelper.doCommand(Command.MUNLOCK, getName(), proxyHelper.toData(key));
    }

    public void forceUnlock(K key) {
        check(key);
        proxyHelper.doCommand(Command.MFORCEUNLOCK, getName(), proxyHelper.toData(key));
    }

    public Collection<V> values(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.values();
//        return new ValueCollection<K, V>(this, set);
    }

    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) proxyHelper.doCommandAsObject(Command.MPUTIFABSENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public boolean remove(Object arg0, Object arg1) {
        check(arg0);
        check(arg1);
        Protocol protocol = proxyHelper.doCommand(Command.MREMOVEIFSAME, new String[]{getName()}, proxyHelper.toData(arg0), proxyHelper.toData(arg1));
        return Boolean.valueOf(protocol.args[0]);
    }

    public V replace(K arg0, V arg1) {
        check(arg0);
        check(arg1);
        return (V) proxyHelper.doCommandAsObject(Command.MREPLACEIFNOTNULL, getName(), proxyHelper.toData(arg0), proxyHelper.toData(arg1));
    }

    public boolean replace(K arg0, V arg1, V arg2) {
        check(arg0);
        check(arg1);
        check(arg2);
        Keys keys = new Keys();
        keys.getKeys().add(proxyHelper.toData(arg1));
        keys.getKeys().add(proxyHelper.toData(arg2));
        Protocol protocol = proxyHelper.doCommand(Command.MREPLACEIFSAME, new String[]{getName()}, proxyHelper.toData(arg0), proxyHelper.toData(arg1), proxyHelper.toData(arg2));
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
        Protocol protocol = proxyHelper.doCommand(Command.MCONTAINSKEY, new String[]{getName()}, proxyHelper.toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean containsValue(Object arg0) {
        check(arg0);
        Protocol protocol = proxyHelper.doCommand(Command.MCONTAINSVALUE, new String[]{getName()}, proxyHelper.toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return entrySet(null);
    }

    public V get(Object key) {
        check(key);
        return (V) proxyHelper.doCommandAsObject(Command.MGET, getName(), proxyHelper.toData(key));
    }

    public Map<K, V> getAll(Set<K> setKeys) {
        check(setKeys);
        List<Data> dataList = new ArrayList<Data>(setKeys.size());
        for (K key : setKeys) {
            dataList.add(proxyHelper.toData(key));
        }
        Map<K, V> map = new HashMap<K, V>(setKeys.size());
        Protocol protocol = proxyHelper.doCommand(Command.MGETALL, new String[]{getName()}, dataList.toArray(new Data[]{}));
        if (protocol.hasBuffer()) {
            int i = 0;
            System.out.println("Get all and buffer length is " + protocol.buffers.length);
            while (i < protocol.buffers.length) {
                K key = (K) proxyHelper.toObject(protocol.buffers[i]);
                i++;
                V value = (V) proxyHelper.toObject(protocol.buffers[i]);
                i++;
                if (value != null) {
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

    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        return null;
    }

    public Set<K> keySet() {
        return keySet(null);
    }

    public Future<V> getAsync(K key) {
        check(key);
        return proxyHelper.doAsync(Command.MGET, new String[]{getName()}, proxyHelper.toData(key));
    }

    public Future<V> putAsync(K key, V value) {
        check(key);
        check(value);
        return proxyHelper.doAsync(Command.MPUT, new String[]{getName()}, proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public Future<V> removeAsync(K key) {
        check(key);
        return proxyHelper.doAsync(Command.MREMOVE, new String[]{getName()}, proxyHelper.toData(key));
    }

    public V put(K key, V value) {
        check(key);
        check(value);
        return (V) proxyHelper.doCommandAsObject(Command.MPUT, getName(), proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) proxyHelper.doCommandAsObject(Command.MPUT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        proxyHelper.doCommand(Command.MSET, new String[]{getName(), "" + timeunit.toMillis(ttl)}, proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        proxyHelper.doCommand(Command.MPUTTRANSIENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, proxyHelper.toData(key), proxyHelper.toData(value));
    }

    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        check(key);
        check(value);
        Protocol protocol = proxyHelper.doCommand(Command.MTRYPUT, new String[]{getName(), "" + timeunit.toMillis(timeout)}, proxyHelper.toData(key), proxyHelper.toData(value));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void putAll(final Map<? extends K, ? extends V> map) {
        List<Data> dataList = new ArrayList<Data>();
        for (K k : map.keySet()) {
            if (k != null) {
                V v = map.get(k);
                if (v != null) {
                    dataList.add(proxyHelper.toData(k));
                    dataList.add(proxyHelper.toData(v));
                }
            }
        }
        proxyHelper.doCommand(Command.MPUTALL, getName(), dataList.toArray(new Data[]{}));
    }

    public V remove(Object arg0) {
        check(arg0);
        return (V) proxyHelper.doCommandAsObject(Command.MREMOVE, getName(), proxyHelper.toData(arg0));
    }

    public Object tryRemove(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Protocol protocol = proxyHelper.doCommand(Command.MTRYREMOVE, new String[]{getName(), "" + timeunit.toMillis(timeout)}, proxyHelper.toData(key));
        if (protocol.args != null && protocol.args.length > 0) {
            if ("timeout".equals(protocol.args[0])) {
                throw new TimeoutException();
            }
        }
        return protocol.hasBuffer() ? (V) proxyHelper.toObject(protocol.buffers[0]) : null;
    }

    public int size() {
        Protocol protocol = proxyHelper.doCommand(Command.MSIZE, new String[]{getName()}, null);
        return Integer.valueOf(protocol.args[0]);
    }

    public Collection<V> values() {
        return values(null);
    }

    public Object getId() {
        return name;
    }

    public void addIndex(String attribute, boolean ordered) {
        proxyHelper.doCommand(Command.MADDINDEX, new String[]{getName(), attribute, String.valueOf(ordered)}, null);
    }

    public void addIndex(Expression<?> expression, boolean ordered) {
        proxyHelper.doCommand(Command.MADDINDEX, new String[]{getName(), String.valueOf(ordered)}, proxyHelper.toData(expression));
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{"map", getName()}, null);
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
