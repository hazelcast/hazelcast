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
import com.hazelcast.client.proxy.nearcache.NearCache;
import com.hazelcast.client.util.EntryHolder;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapService;
import com.hazelcast.map.SimpleEntryView;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.String.valueOf;

public class MapClientProxy<K, V> implements IMap<K, V>, EntryHolder<K, V> {
    final ProxyHelper proxyHelper;
    final private String name;
    final HazelcastClient client;
    final Map<EntryListener<K, V>, Map<Object, ListenerThread>> listenerMap = new IdentityHashMap<EntryListener<K, V>, Map<Object, ListenerThread>>();
    final NearCache<K, V> nearCache;

    public MapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.client = client;
        this.proxyHelper = new ProxyHelper(client);
        NearCacheConfig nc = client.getClientConfig().getNearCacheConfig(name);
        nearCache = (nc == null) ? null : new NearCache<K, V>(this, nc);
    }



    public void flush() {
        proxyHelper.doCommand(Command.MFLUSH, new String[]{getName()});
    }

    public void addInterceptor(MapInterceptor interceptor) {
        Data dInterceptor = proxyHelper.toData(interceptor);
        proxyHelper.doCommand(Command.MADDINTERCEPTOR, new String[]{getName()}, dInterceptor);
    }

    public void removeInterceptor(MapInterceptor interceptor) {
        Data dInterceptor = proxyHelper.toData(interceptor);
        proxyHelper.doCommand(Command.MREMOVEINTERCEPTOR, new String[]{getName()}, dInterceptor);
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
        Data[] datas = dKey == null ? null : new Data[]{dKey};
        Protocol request = proxyHelper.createProtocol(Command.MLISTEN, new String[]{name, valueOf(includeValue)}, datas);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.mapListener.",
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

    public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.entrySet();
    }

    private Map<K, V> getCopyOfTheMap(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = proxyHelper.doCommand(Command.MENTRYSET, new String[]{getName()});
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

    public boolean evict(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MEVICT, new String[]{getName()}, dKey);
        Boolean evicted = Boolean.valueOf(protocol.args[0]);
        return evicted;
    }

    public EntryView<K, V> getEntryView(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        final Protocol protocol = proxyHelper.doCommand(dKey, Command.MGETENTRYVIEW, new String[]{getName()}, dKey);
        if (!protocol.hasBuffer()) return new SimpleEntryView<K, V>();
        else {
            String[] args = protocol.args;
            return new EntryView<K, V>() {
                @Override
                public K getKey() {
                    return (K)proxyHelper.toObject(protocol.buffers[0]);
                }

                @Override
                public V getValue() {
                    return (V)proxyHelper.toObject(protocol.buffers[1]);
                }

                @Override
                public long getCost() {
                    return Long.valueOf(protocol.args[0]);
                }

                @Override
                public long getCreationTime() {
                    return Long.valueOf(protocol.args[1]);
                }

                @Override
                public long getExpirationTime() {
                    return Long.valueOf(protocol.args[2]);
                }

                @Override
                public long getHits() {
                    return Long.valueOf(protocol.args[3]);
                }

                @Override
                public long getLastAccessTime() {
                    return Long.valueOf(protocol.args[4]);
                }

                @Override
                public long getLastStoredTime() {
                    return Long.valueOf(protocol.args[5]);
                }

                @Override
                public long getLastUpdateTime() {
                    return Long.valueOf(protocol.args[6]);
                }

                @Override
                public long getVersion() {
                    return Long.valueOf(protocol.args[7]);
                }
            };
        }
        
    }

    public Set<K> keySet(Predicate predicate) {
        Protocol protocol;
        if (predicate == null)
            protocol = proxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()});
        else
            protocol = proxyHelper.doCommand(Command.KEYSET, new String[]{"map", getName()}, proxyHelper.toData(predicate));
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
        proxyHelper.lock(getName(), dKey, Command.MLOCK, new String[]{getName()}, dKey);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.lock(getName(), dKey, Command.MLOCK, new String[]{getName(), String.valueOf(timeUnit.toMillis(leaseTime))}, dKey);
    }

    public boolean isLocked(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MISLOCKED, new String[]{getName()}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(K key) {
        return tryLock(key, 0, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        String[] args = new String[]{getName(), "" + timeunit.toMillis(time)};
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYLOCK, args, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.unlock(getName(), dKey, Command.MUNLOCK, new String[]{getName()}, dKey);
    }

    public void forceUnlock(K key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MFORCEUNLOCK, new String[]{getName()}, dKey);
    }

    public Collection<V> values(Predicate predicate) {
        Map<K, V> map = getCopyOfTheMap(predicate);
        return map.values();
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

    public void delete(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MDELETE, new String[]{getName()}, dKey);
    }

    public V replace(K key, V value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MREPLACEIFNOTNULL, new String[]{getName()}, dKey, proxyHelper.toData(value));
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
        Protocol protocol = proxyHelper.doCommand(Command.MCONTAINSVALUE, new String[]{getName()}, proxyHelper.toData(arg0));
        return Boolean.valueOf(protocol.args[0]);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return entrySet(null);
    }

    public V get(Object key) {
        check(key);
        if (nearCache != null) {
            V cachedValue = nearCache.get(key);
            if (cachedValue != null) return cachedValue;
        }
        Data dKey = proxyHelper.toData(key);
        V result = (V) proxyHelper.doCommandAsObject(dKey, Command.MGET, new String[]{getName()}, dKey);
        if (nearCache != null) nearCache.put(key, result);
        return result;
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
        Data dKey = proxyHelper.toData(key);
        Data dEntryProcessor = proxyHelper.toData(entryProcessor);
        return proxyHelper.doCommandAsObject(dKey, Command.MEXECUTEONKEY, new String[]{getName()}, dKey, dEntryProcessor);
    }

    public Map executeOnAllKeys(EntryProcessor entryProcessor) {
        Data dEntryProcessor = proxyHelper.toData(entryProcessor);
        return (Map) proxyHelper.doCommandAsObject(Command.MEXECUTEONALLKEYS, new String[]{getName()}, dEntryProcessor);
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
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MPUT, new String[]{getName()}, dKey, proxyHelper.toData(value));
    }

    public void set(K key, V value) {
        check(key);
        check(value);
        Data dKey = proxyHelper.toData(key);
        proxyHelper.doCommand(dKey, Command.MSET, new String[]{getName()}, dKey, proxyHelper.toData(value));
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
        proxyHelper.doCommand(Command.MPUTALL, new String[]{getName()}, dataList.toArray(new Data[]{}));
    }

    public V remove(Object key) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        return (V) proxyHelper.doCommandAsObject(dKey, Command.MREMOVE, new String[]{getName()}, dKey);
    }

    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        check(key);
        Data dKey = proxyHelper.toData(key);
        Protocol protocol = proxyHelper.doCommand(dKey, Command.MTRYREMOVE, new String[]{getName(), "" + timeunit.toMillis(timeout)}, dKey);
        return Boolean.valueOf(protocol.args[0]);
    }

    public int size() {
        Protocol protocol = proxyHelper.doCommand(Command.MSIZE, new String[]{getName()});
        return Integer.valueOf(protocol.args[0]);
    }

    public Collection<V> values() {
        return values(null);
    }

    public Object getId() {
        return name;
    }

    public void addIndex(String attribute, boolean ordered) {
        proxyHelper.doCommand(Command.MADDINDEX, new String[]{getName(), attribute, String.valueOf(ordered)});
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{MapService.SERVICE_NAME, getName()});
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
