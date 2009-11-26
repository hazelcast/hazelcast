package com.hazelcast.impl;

import com.hazelcast.core.*;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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

        public V put(K key, V value, long ttl, TimeUnit timeunit) {
            return distributedMap.put(key, value, ttl, timeunit);
        }

        @Override
        public V put(K key, V value) {
            return distributedMap.put(key, value);
        }

        @Override
        public V remove(Object key) {
            return distributedMap.remove(key);
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

        public MapEntry getMapEntry(K key) {
            return distributedMap.getMapEntry(key);
        }

        public boolean evict(K key) {
            return distributedMap.evict(key);
        }

        public Set<K> keySet(Predicate predicate) {
            Set<K> result = new HashSet<K>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (predicate.apply((MapEntry) entry)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        public Set<Entry<K, V>> entrySet(Predicate predicate) {
            Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (predicate.apply((MapEntry) entry)) {
                    result.add(entry);
                }
            }
            return result;
        }

        public Collection<V> values(Predicate predicate) {
            Set<V> result = new HashSet<V>();
            Set<Map.Entry<K, V>> entries = super.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (predicate.apply((MapEntry) entry)) {
                    result.add(entry.getValue());
                }
            }
            return result;
        }

        public <K> Set<K> localKeySet() {
            return distributedMap.localKeySet();
        }

        public <K> Set<K> localKeySet(Predicate predicate) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void addIndex(String attribute, boolean ordered) {
            distributedMap.addIndex(attribute, ordered);
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