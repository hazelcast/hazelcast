/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.adapter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.transaction.TransactionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TransactionalMapDataStructureAdapter<K, V> implements DataStructureAdapter<K, V> {

    private final HazelcastInstance hazelcastInstance;
    private final String name;

    private TransactionContext transactionContext;
    private TransactionalMap<K, V> transactionalMap;

    public TransactionalMapDataStructureAdapter(HazelcastInstance hazelcastInstance, String name) {
        this.hazelcastInstance = hazelcastInstance;
        this.name = name;
    }

    @Override
    public void clear() {
        begin();
        for (K key : transactionalMap.keySet()) {
            transactionalMap.remove(key);
        }
        commit();
    }

    @Override
    public void set(K key, V value) {
        begin();
        transactionalMap.put(key, value);
        commit();
    }

    @Override
    public V put(K key, V value) {
        begin();
        V oldValue = transactionalMap.put(key, value);
        commit();
        return oldValue;
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        begin();
        V oldValue = transactionalMap.putIfAbsent(key, value);
        commit();
        return oldValue == null;
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        begin();
        V oldValue = transactionalMap.putIfAbsent(key, value);
        commit();
        return new SimpleCompletedFuture<Boolean>(oldValue == null);
    }

    @Override
    public V replace(K key, V newValue) {
        begin();
        V oldValue = transactionalMap.replace(key, newValue);
        commit();
        return oldValue;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        begin();
        boolean result = transactionalMap.replace(key, oldValue, newValue);
        commit();
        return result;
    }

    @Override
    public V get(K key) {
        begin();
        V value = transactionalMap.get(key);
        commit();
        return value;
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        begin();
        V value = transactionalMap.get(key);
        commit();
        return new SimpleCompletedFuture<V>(value);
    }

    @Override
    public void putAll(Map<K, V> map) {
        begin();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            transactionalMap.put(entry.getKey(), entry.getValue());
        }
        commit();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        begin();
        Map<K, V> result = new HashMap<K, V>(keys.size());
        for (K key : keys) {
            result.put(key, transactionalMap.get(key));
        }
        commit();
        return result;
    }

    @Override
    public void remove(K key) {
        begin();
        transactionalMap.remove(key);
        commit();
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        begin();
        V value = transactionalMap.remove(key);
        commit();
        return new SimpleCompletedFuture<V>(value);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        begin();
        boolean result = transactionalMap.containsKey(key);
        commit();
        return result;
    }

    private void begin() {
        transactionContext = hazelcastInstance.newTransactionContext();
        transactionContext.beginTransaction();
        transactionalMap = transactionContext.getMap(name);
    }

    private void commit() {
        transactionContext.commitTransaction();
        transactionContext = null;
        transactionalMap = null;
    }
}
