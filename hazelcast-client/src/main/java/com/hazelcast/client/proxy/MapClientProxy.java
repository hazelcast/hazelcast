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
import com.hazelcast.client.proxy.listener.EntryEventLRH;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.util.EntryHolder;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.*;

public class MapClientProxy<K, V> implements IMap<K, V>, EntryHolder {
    final ProxyHelper proxyHelper;
    final private String name;
    final HazelcastClient client;
    final Map<EntryListener<K, V>, Map<Object, ListenerThread>> listenerMap = new IdentityHashMap<EntryListener<K, V>, Map<Object, ListenerThread>>();
    final static AtomicInteger threadCounter = new AtomicInteger(0);
    public MapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.client = client;
        this.proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
    }

    public void flush(boolean flushAllEntries) {
    }

    public void addInterceptor(MapInterceptor interceptor) {
    }

    public void removeInterceptor(MapInterceptor interceptor) {
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(final EntryListener<K, V> entryListener, final Predicate<K, V> predicate, final K key,
                                 final boolean includeValue) {
    }

    public void addEntryListener(final EntryListener<K, V> listener, final K key, final boolean includeValue) {
        Data dKey = key == null ? null : proxyHelper.toData(key);
        Protocol request = proxyHelper.createProtocol(Command.MLISTEN, new String[]{name,valueOf(includeValue)}, new Data[]{dKey});
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.mapListener." + threadCounter.incrementAndGet(),
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

    public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.entrySet();
    }

    private Map<K, V> getCopyOfTheMap(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = proxyHelper.doCommand(null, Command.MENTRYSET, new String[]{getName()}, null);
        else
            protocol = proxyHelper.doCommand(null, Command.MENTRYSET, new String[]{getName()}, proxyHelper.toData(predicate));
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
        proxyHelper.doCommand(null, Command.MFLUSH, getName(), null);
    }

    public boolean evict(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MEVICT, new String[]{getName()}, dKey);
        Boolean evicted = Boolean.valueOf(protocol.args[0]);
        return evicted;
    }

    public MapEntry<K, V> getMapEntry(final K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MGETENTRY, new String[]{getName()}, dKey);
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
            protocol = proxyHelper.doCommand(null, Command.KEYSET, new String[]{"map", getName()}, null);
        else
            protocol = proxyHelper.doCommand(null, Command.KEYSET, new String[]{"map", getName()}, proxyHelper.toData(predicate));
        if (!protocol.hasBuffer()) return Collections.emptySet();
        Set<K> set = new HashSet<K>(protocol.buffers.length);
        for (Data b : protocol.buffers) {
            set.add((K) proxyHelper.toObject(b));
        }
        return set;
    }

    public void lock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MLOCK, getName(), dKey);
    }

    public boolean isLocked(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MISLOCKED, new String[]{getName()}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYLOCK, new String[]{getName(), "0"}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYLOCK, new String[]{getName(), "" + timeunit.toMillis(time)}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MUNLOCK, getName(), dKey);
    }

    public void forceUnlock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MFORCEUNLOCK, getName(), dKey);
    }

    public Collection<V> values(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.values();
//        return new ValueCollection<K, V>(this, set);
    }

    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MPUTIFABSENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, dKey, proxyHelper.toData(value));
    }

    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public boolean remove(Object key, Object value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MREMOVEIFSAME, new String[]{getName()}, dKey, proxyHelper.toData(value));
        return Boolean.valueOf(protocol.args[0]);
    }

    public V replace(K key, V value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MREPLACEIFNOTNULL, getName(), dKey, proxyHelper.toData(value));
    }

    public boolean replace(K key, V oldValue, V newValue) {
        check(key);
        check(oldValue);
        check(newValue);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MREPLACEIFSAME, new String[]{getName()}, dKey, proxyHelper.toData(oldValue), proxyHelper.toData(newValue));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public boolean containsKey(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MCONTAINSKEY, new String[]{getName()}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean containsValue(Object arg0) {
        check(arg0);
        Protocol protocol = proxyHelper.doCommand(null, Command.MCONTAINSVALUE, new String[]{getName()}, proxyHelper.toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return entrySet(null);
    }

    public V get(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MGET, getName(), dKey);
    }

    public Map<K, V> getAll(Set<K> setKeys) {
        check(setKeys);
        List<Data> dataList = new ArrayList<Data>(setKeys.size());
        for (K key : setKeys) {
            dataList.add(proxyHelper.toData(key));
        }
        Map<K, V> map = new HashMap<K, V>(setKeys.size());
        Protocol protocol = proxyHelper.doCommand(null, Command.MGETALL, new String[]{getName()}, dataList.toArray(new Data[]{}));
        if (protocol.hasBuffer()) {
            int i = 0;
            while (i < protocol.buffers.length) {
                K key = (K) proxyHelper.toObject(protocol.buffers[i++]);
                V value = (V) proxyHelper.toObject(protocol.buffers[i++]);
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

    public void cleanUpNearCache() {
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
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MPUT, getName(), dKey, proxyHelper.toData(value));
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MPUT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, dKey, proxyHelper.toData(value));
    }

    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MSET, new String[]{getName(), "" + timeunit.toMillis(ttl)}, dKey, proxyHelper.toData(value));
    }

    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MPUTTRANSIENT, new String[]{getName(), "" + timeunit.toMillis(ttl)}, dKey, proxyHelper.toData(value));
    }

    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYPUT, new String[]{getName(), "" + timeunit.toMillis(timeout)}, dKey, proxyHelper.toData(value));
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
        proxyHelper.doCommand(null, Command.MPUTALL, getName(), dataList.toArray(new Data[]{}));
    }

    public V remove(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MREMOVE, getName(), dKey);
    }

    public Object tryRemove(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYREMOVE, new String[]{getName(), "" + timeunit.toMillis(timeout)}, dKey);
        if (protocol.args != null && protocol.args.length > 0) {
            if ("timeout".equals(protocol.args[0])) {
                throw new TimeoutException();
            }
        }
        return protocol.hasBuffer() ? (V) proxyHelper.toObject(protocol.buffers[0]) : null;
    }

    public int size() {
        Protocol protocol = proxyHelper.doCommand(null, Command.MSIZE, new String[]{getName()}, null);
        return Integer.valueOf(protocol.args[0]);
    }

    public Collection<V> values() {
        return values(null);
    }

    public Object getId() {
        return name;
    }

    public void addIndex(String attribute, boolean ordered) {
        proxyHelper.doCommand(null, Command.MADDINDEX, new String[]{getName(), attribute, String.valueOf(ordered)}, null);
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(null, Command.DESTROY, new String[]{"map", getName()}, null);
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
