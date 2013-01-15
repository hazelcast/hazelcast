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
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.CMap.CMapEntry;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.Keys;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.DistributedTimeoutException;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.client.ProxyHelper.check;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class MapClientProxy<K, V> implements IMap<K, V>, EntryHolder {
    final ProxyHelper proxyHelper;
    final private String name;
    final NearCache<K, V> nearCache;
    private static String PROP_CLIENT_NEAR_CACHE_CONFIG_ENABLED = "hazelcast.client.near.cache.enabled";

    public MapClientProxy(HazelcastClient client, String name) {
        this.name = name;
        this.proxyHelper = new ProxyHelper(name, client);
        Config config = (Config) proxyHelper.doOp(ClusterOperation.GET_CONFIG, null, null);
        MapConfig mapConfig = config.getMapConfig(name);
        NearCacheConfig ncc = mapConfig.getNearCacheConfig();

        boolean nearCacheEnabled = "true".equalsIgnoreCase(System.getProperty(PROP_CLIENT_NEAR_CACHE_CONFIG_ENABLED, "false"));
        nearCache = (nearCacheEnabled && ncc != null) ? new GuavaNearCacheImpl<K, V>(ncc, this) : null;
        if (nearCache != null) {
            if (ncc.isInvalidateOnChange()) {
                addEntryListener(new EntryListener<K, V>() {
                    public void entryAdded(EntryEvent<K, V> kvEntryEvent) {
                    }

                    public void entryRemoved(EntryEvent<K, V> kvEntryEvent) {
                        nearCache.invalidate(kvEntryEvent.getKey());
                    }

                    public void entryUpdated(EntryEvent<K, V> kvEntryEvent) {
                        nearCache.invalidate(kvEntryEvent.getKey());
                    }

                    public void entryEvicted(EntryEvent<K, V> kvEntryEvent) {
                        nearCache.invalidate(kvEntryEvent.getKey());
                    }
                }, false);
            }
        }
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("client doesn't support local entry listener");
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        addEntryListener(listener, null, includeValue);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        check(listener);
        Boolean noEntryListenerRegistered = listenerManager().noListenerRegistered(key, name, includeValue);
        if (noEntryListenerRegistered == null) {
            proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, key, null);
            noEntryListenerRegistered = Boolean.TRUE;
        }
        if (noEntryListenerRegistered) {
            Call c = listenerManager().createNewAddListenerCall(proxyHelper, key, includeValue);
            proxyHelper.doCall(c);
        }
        listenerManager().registerListener(name, key, includeValue, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        check(listener);
        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, null, null);
        listenerManager().removeListener(name, null, listener);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        check(listener);
        check(key);
        proxyHelper.doOp(ClusterOperation.REMOVE_LISTENER, key, null);
        listenerManager().removeListener(name, key, listener);
    }

    private EntryListenerManager listenerManager() {
        return proxyHelper.getHazelcastClient().getListenerManager().getEntryListenerManager();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet(Predicate predicate) {
        final Collection collection = proxyHelper.entries(predicate);
        return new LightEntrySetSet<K, V>(collection, this, getInstanceType());
    }

    public void flush() {
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_FLUSH, null, null);
    }

    public boolean evict(Object key) {
        ProxyHelper.check(key);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_EVICT, key, null);
    }

    public MapEntry<K, V> getMapEntry(K key) {
        ProxyHelper.check(key);
        CMapEntry cMapEntry = (CMapEntry) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_GET_MAP_ENTRY, key, null);
        if (cMapEntry == null) {
            return null;
        }
        return new ClientMapEntry(cMapEntry, key, this);
    }

    public Set<K> keySet(Predicate predicate) {
        final Collection<K> collection = proxyHelper.keys(predicate);
        return new LightKeySet<K>(this, new HashSet<K>(collection));
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        ProxyHelper.checkTime(time, timeunit);
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_LOCK_MAP, null, time, timeunit);
    }

    public void unlockMap() {
        doLock(ClusterOperation.CONCURRENT_MAP_UNLOCK_MAP, null, -1, null);
    }

    public void lock(K key) {
        ProxyHelper.check(key);
        doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, -1, null);
    }

    public boolean isLocked(K key) {
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_IS_KEY_LOCKED, key, -1, null);
    }

    public boolean tryLock(K key) {
        check(key);
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, 0, null);
    }

    public V tryLockAndGet(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Object result = proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_TRY_LOCK_AND_GET, key, null, timeout, timeunit);
        if (result instanceof DistributedTimeoutException) {
            throw new TimeoutException();
        }
        return (V) result;
    }

    public void putAndUnlock(K key, V value) {
        check(key);
        check(value);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_AND_UNLOCK, key, value);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        check(key);
        ProxyHelper.checkTime(time, timeunit);
        return (Boolean) doLock(ClusterOperation.CONCURRENT_MAP_LOCK, key, time, timeunit);
    }

    public void unlock(K key) {
        check(key);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_UNLOCK, key, null);
    }

    public void forceUnlock(K key) {
        check(key);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_FORCE_UNLOCK, key, null);
    }

    public Collection<V> values(Predicate predicate) {
        Set<Entry<K, V>> set = entrySet(predicate);
        return new ValueCollection<K, V>(this, set);
    }

    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT, key, value, ttl, timeunit);
    }

    public V putIfAbsent(K key, V value) {
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT, key, value);
    }

    public boolean remove(Object arg0, Object arg1) {
        check(arg0);
        check(arg1);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_IF_SAME, arg0, arg1);
    }

    public V replace(K arg0, V arg1) {
        check(arg0);
        check(arg1);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REPLACE_IF_NOT_NULL, arg0, arg1);
    }

    public boolean replace(K arg0, V arg1, V arg2) {
        check(arg0);
        check(arg1);
        check(arg2);
        Keys keys = new Keys();
        keys.getKeys().add(toData(arg1));
        keys.getKeys().add(toData(arg2));
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REPLACE_IF_SAME, arg0, keys);
    }

    public void clear() {
        Set keys = keySet();
        for (Object key : keys) {
            remove(key);
        }
    }

    public boolean containsKey(Object arg0) {
        check(arg0);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_KEY, arg0, null);
    }

    public boolean containsValue(Object arg0) {
        check(arg0);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, null, arg0);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return entrySet(null);
    }

    public V get(Object key) {
        if (nearCache != null)
            return nearCache.get((K) key);
        return get0(key);
    }

    protected V get0(Object key) {
        check(key);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_GET, (K) key, null);
    }

    public Map<K, V> getAll(Set<K> setKeys) {
        Keys keys = new Keys();
        for (K key : setKeys) {
            keys.add(toData(key));
        }
        Pairs pairs = (Pairs) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_GET_ALL, keys, null);
        List<KeyValue> lsKeyValues = pairs.getKeyValues();
        Map map = new HashMap();
        if (lsKeyValues != null) {
            for (KeyValue keyValue : lsKeyValues) {
                map.put(toObject(keyValue.getKeyData()), toObject(keyValue.getValueData()));
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
        return proxyHelper.doAsync(ClusterOperation.CONCURRENT_MAP_GET, key, null);
    }

    public Future<V> putAsync(K key, V value) {
        check(key);
        check(value);
        return proxyHelper.doAsync(ClusterOperation.CONCURRENT_MAP_PUT, key, value);
    }

    public Future<V> removeAsync(K key) {
        check(key);
        return proxyHelper.doAsync(ClusterOperation.CONCURRENT_MAP_REMOVE, key, null);
    }

    public V put(K key, V value) {
        check(key);
        check(value);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT, key, value);
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT, key, value, ttl, timeunit);
    }

    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SET, key, value, ttl, timeunit);
    }

    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        check(key);
        check(value);
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_TRANSIENT, key, value, ttl, timeunit);
    }

    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        check(key);
        check(value);
        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_TRY_PUT, key, value, timeout, timeunit);
    }

    public void putAll(final Map<? extends K, ? extends V> map) {
        Pairs pairs = new Pairs(map.size());
        for (final K key : map.keySet()) {
            final V value = map.get(key);
            pairs.addKeyValue(new KeyValue(toData(key), toData(value)));
        }
        proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_ALL, null, pairs);
    }

    public V remove(Object arg0) {
        check(arg0);
        return (V) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE, arg0, null);
    }

    public Object tryRemove(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
        check(key);
        Object result = proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_TRY_REMOVE, key, null, timeout, timeunit);
        if (result instanceof DistributedTimeoutException) {
            throw new TimeoutException();
        }
        return result;
    }

    private Object doLock(ClusterOperation operation, Object key, long timeout, TimeUnit timeUnit) {
        Packet request = proxyHelper.prepareRequest(operation, key, timeUnit);
        request.setTimeout(timeout);
        Packet response = proxyHelper.callAndGetResult(request);
        return proxyHelper.getValue(response);
    }

    public int size() {
        return (Integer) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
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
        proxyHelper.doOp(ClusterOperation.ADD_INDEX, attribute, ordered);
    }

    public void addIndex(Expression<?> expression, boolean ordered) {
        proxyHelper.doOp(ClusterOperation.ADD_INDEX, expression, ordered);
    }

    public String getName() {
        return name.substring(Prefix.MAP.length());
    }

    public void destroy() {
        proxyHelper.destroy();
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

    public NearCache<K, V> getNearCache() {
        return nearCache;
    }
}
