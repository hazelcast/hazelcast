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

package com.hazelcast.impl;

import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("SynchronizationOnStaticField")
public final class ReplicatedMapFactory {

    private static final ConcurrentHashMap<String, IMap> maps = new ConcurrentHashMap<String, IMap>(100);
    private static final Object factoryLock = new Object();

    private ReplicatedMapFactory() {
    }

    public static IMap getMap(String name) {
        IMap map = maps.get(name);
        if (map == null) {
            synchronized (factoryLock) {
                map = maps.get(name);
                if (map == null) {
                    map = new ReplicatedMap(name);
                }
            }
        }
        return map;
    }

    private static class ReplicatedMap<K, V> extends ConcurrentHashMap<K, V> implements EntryListener<K, V>, IMap<K, V> {
        final IMap<K, V> distributedMap;

        public ReplicatedMap(String name) {
            distributedMap = Hazelcast.getMap(name);
            distributedMap.addEntryListener(this, true);
            Set<K> keys = distributedMap.keySet();
            for (K key : keys) {
                V value = distributedMap.get(key);
                if (value != null) {
                    super.putIfAbsent(key, value);
                }
            }
            Set<Entry<K, V>> entries = distributedMap.entrySet();
            for (Entry<K, V> entry : entries) {
                this.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        public void flush() {
            distributedMap.flush();
        }

        public void putAndUnlock(K key, V value) {
            distributedMap.putAndUnlock(key, value);
        }

        public V tryLockAndGet(K key, long time, TimeUnit timeunit) throws TimeoutException {
            return distributedMap.tryLockAndGet(key, time, timeunit);
        }

        public Map<K, V> getAll(Set<K> keys) {
            Map map = new HashMap();
            for (K key : keys) {
                map.put(key, get(key));
            }
            return map;
        }

        public Future<V> getAsync(K key) {
            return distributedMap.getAsync(key);
        }

        public Future<V> putAsync(K key, V value) {
            return distributedMap.putAsync(key, value);
        }

        public Future<V> removeAsync(K key) {
            return distributedMap.removeAsync(key);
        }

        public V put(K key, V value, long ttl, TimeUnit timeunit) {
            return distributedMap.put(key, value, ttl, timeunit);
        }

        public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
            distributedMap.put(key, value, ttl, timeunit);
        }

        @Override
        public V put(K key, V value) {
            return distributedMap.put(key, value);
        }

        public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
            return distributedMap.tryPut(key, value, timeout, timeunit);
        }

        @Override
        public V remove(Object key) {
            return distributedMap.remove(key);
        }

        public Object tryRemove(K key, long timeout, TimeUnit timeunit) throws TimeoutException {
            return distributedMap.tryRemove(key, timeout, timeunit);
        }

        @Override
        public V putIfAbsent(K key, V value) {
            V currentValue = super.get(key);
            if (currentValue != null) {
                return currentValue;
            }
            return distributedMap.putIfAbsent(key, value);
        }

        public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
            V currentValue = super.get(key);
            if (currentValue != null) {
                return currentValue;
            }
            return distributedMap.putIfAbsent(key, value, ttl, timeunit);
        }

        public void entryAdded(EntryEvent<K, V> event) {
            super.put(event.getKey(), event.getValue());
        }

        public void entryRemoved(EntryEvent<K, V> event) {
            super.remove(event.getKey());
        }

        public void entryUpdated(EntryEvent<K, V> event) {
            super.put(event.getKey(), event.getValue());
        }

        public void entryEvicted(EntryEvent<K, V> event) {
            super.remove(event.getKey());
        }

        public String getName() {
            return distributedMap.getName();
        }

        public void lock(K key) {
            distributedMap.lock(key);
        }

        public boolean tryLock(K key) {
            return distributedMap.tryLock(key);
        }

        public boolean tryLock(K key, long time, TimeUnit timeunit) {
            return distributedMap.tryLock(key, time, timeunit);
        }

        public void unlock(K key) {
            distributedMap.unlock(key);
        }

        public void forceUnlock(K key) {
            distributedMap.forceUnlock(key);
        }

        public boolean lockMap(long time, TimeUnit timeunit) {
            return distributedMap.lockMap(time, timeunit);
        }

        public void unlockMap() {
            distributedMap.unlockMap();
        }

        public LocalMapStats getLocalMapStats() {
            return distributedMap.getLocalMapStats();
        }

        public void addLocalEntryListener(EntryListener<K, V> entryListener) {
            distributedMap.addLocalEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
            distributedMap.addEntryListener(listener, includeValue);
        }

        public void removeEntryListener(EntryListener<K, V> listener) {
            distributedMap.removeEntryListener(listener);
        }

        public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
            distributedMap.addEntryListener(listener, key, includeValue);
        }

        public void removeEntryListener(EntryListener<K, V> listener, K key) {
            distributedMap.removeEntryListener(listener, key);
        }

        public MapEntry<K, V> getMapEntry(K key) {
            return distributedMap.getMapEntry(key);
        }

        public boolean evict(Object key) {
            return distributedMap.evict(key);
        }

        public Set<K> keySet(Predicate predicate) {
            Set<K> result = new HashSet<K>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (apply(predicate, entry)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        private boolean apply(Predicate predicate, final Map.Entry entry) {
            return predicate.apply(new MapEntry() {
                public long getCost() {
                    return 0;
                }

                public long getCreationTime() {
                    return 0;
                }

                public long getExpirationTime() {
                    return 0;
                }

                public int getHits() {
                    return 0;
                }

                public long getLastAccessTime() {
                    return 0;
                }

                public long getLastStoredTime() {
                    return 0;
                }

                public long getLastUpdateTime() {
                    return 0;
                }

                public long getVersion() {
                    return 0;
                }

                public boolean isValid() {
                    return true;
                }

                public Object getKey() {
                    return entry.getKey();
                }

                public Object getValue() {
                    return entry.getValue();
                }

                public Object setValue(Object value) {
                    return entry.setValue(value);
                }
            });
        }

        public Set<Entry<K, V>> entrySet(Predicate predicate) {
            Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (apply(predicate, entry)) {
                    result.add(entry);
                }
            }
            return result;
        }

        public Collection<V> values(Predicate predicate) {
            Set<V> result = new HashSet<V>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (apply(predicate, entry)) {
                    result.add(entry.getValue());
                }
            }
            return result;
        }

        public Set<K> localKeySet() {
            return distributedMap.localKeySet();
        }

        public Set<K> localKeySet(Predicate predicate) {
            return distributedMap.localKeySet(predicate);
        }

        public void addIndex(String attribute, boolean ordered) {
            distributedMap.addIndex(attribute, ordered);
        }

        public void addIndex(Expression<?> expression, boolean ordered) {
            distributedMap.addIndex(expression, ordered);
        }

        public InstanceType getInstanceType() {
            return distributedMap.getInstanceType();
        }

        public void destroy() {
            distributedMap.destroy();
            clear();
        }

        public Object getId() {
            return distributedMap.getId();
        }
    }
}
