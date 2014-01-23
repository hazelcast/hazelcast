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

package com.hazelcast.client.txn.proxy;

import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapValueCollection;
import com.hazelcast.map.client.TxnMapRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ali 6/10/13
 */
public class ClientTxnMapProxy<K, V> extends ClientTxnProxy implements TransactionalMap<K, V> {

    public ClientTxnMapProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    public boolean containsKey(Object key) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.CONTAINS_KEY, toData(key), threadId);
        Boolean result = invoke(request);
        return result;
    }

    public V get(Object key) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.GET, toData(key), threadId);
        return invoke(request);
    }

    public int size() {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.SIZE, threadId);
        Integer result = invoke(request);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public V put(K key, V value) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT, toData(key), toData(value), threadId);
        return invoke(request);
    }

    public void set(K key, V value) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.SET, toData(key), toData(value), threadId);
        invoke(request);
    }

    public V putIfAbsent(K key, V value) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT_IF_ABSENT, toData(key), toData(value), threadId);
        return invoke(request);
    }

    public V replace(K key, V value) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REPLACE, toData(key), toData(value), threadId);
        return invoke(request);
    }

    public boolean replace(K key, V oldValue, V newValue) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REPLACE_IF_SAME, toData(key), toData(oldValue), toData(newValue), threadId);
        Boolean result = invoke(request);
        return result;
    }

    public V remove(Object key) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REMOVE, toData(key), threadId);
        return invoke(request);
    }

    public void delete(Object key) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.DELETE, toData(key), threadId);
        invoke(request);
    }

    public boolean remove(Object key, Object value) {
        int threadId = (int)Thread.currentThread().getId();
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REMOVE_IF_SAME, toData(key), toData(value), threadId);
        Boolean result = invoke(request);
        return result;
    }

    public Set<K> keySet() {
        int threadId = (int)Thread.currentThread().getId();
        final TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.KEYSET, threadId);
        final MapKeySet result = invoke(request);
        final Set<Data> dataKeySet = result.getKeySet();
        final HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    public Set<K> keySet(Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        int threadId = (int)Thread.currentThread().getId();
        final TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.KEYSET_BY_PREDICATE, predicate, threadId);
        final MapKeySet result = invoke(request);
        final Set<Data> dataKeySet = result.getKeySet();
        final HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    public Collection<V> values() {
        int threadId = (int)Thread.currentThread().getId();
        final TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.VALUES, threadId);
        final MapValueCollection result = invoke(request);
        final Collection<Data> dataValues = result.getValues();
        final HashSet<V> values = new HashSet<V>(dataValues.size());
        for (Data value : dataValues) {
            values.add((V) toObject(value));
        }
        return values;
    }

    public Collection<V> values(Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        int threadId = (int)Thread.currentThread().getId();
        final TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.VALUES_BY_PREDICATE, predicate, threadId);
        final MapValueCollection result = invoke(request);
        final Collection<Data> dataValues = result.getValues();
        final HashSet<V> values = new HashSet<V>(dataValues.size());
        for (Data value : dataValues) {
            values.add((V) toObject(value));
        }
        return values;
    }


    public String getName() {
        return (String) getId();
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    void onDestroy() {
    }
}
