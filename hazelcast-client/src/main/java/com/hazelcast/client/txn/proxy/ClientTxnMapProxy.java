/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapValueCollection;
import com.hazelcast.map.impl.client.TxnMapRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Proxy implementation of {@link com.hazelcast.core.TransactionalMap} interface.
 */
public class ClientTxnMapProxy<K, V> extends ClientTxnProxy implements TransactionalMap<K, V> {

    public ClientTxnMapProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    @Override
    public boolean containsKey(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.CONTAINS_KEY, toData(key));
        return this.<Boolean>invoke(request);
    }

    @Override
    public V get(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.GET, toData(key));
        return invoke(request);
    }

    @Override
    public V getForUpdate(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.GET_FOR_UPDATE, toData(key));
        return invoke(request);
    }

    @Override
    public int size() {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.SIZE);
        return this.<Integer>invoke(request);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public V put(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT, toData(key), toData(value));
        return invoke(request);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT_WITH_TTL, toData(key),
                toData(value), ttl, timeunit);
        return invoke(request);
    }

    @Override
    public void set(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.SET, toData(key), toData(value));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT_IF_ABSENT, toData(key),
                toData(value));
        return invoke(request);
    }

    @Override
    public V replace(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REPLACE, toData(key), toData(value));
        return invoke(request);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REPLACE_IF_SAME, toData(key),
                toData(oldValue), toData(newValue));
        return this.<Boolean>invoke(request);
    }

    @Override
    public V remove(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REMOVE, toData(key));
        return invoke(request);
    }

    @Override
    public void delete(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.DELETE, toData(key));
        invoke(request);
    }

    @Override
    public boolean remove(Object key, Object value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.REMOVE_IF_SAME, toData(key),
                toData(value));
        return this.<Boolean>invoke(request);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.KEYSET);
        MapKeySet result = invoke(request);
        Set<Data> dataKeySet = result.getKeySet();
        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.KEYSET_BY_PREDICATE, predicate);
        MapKeySet result = invoke(request);
        Set<Data> dataKeySet = result.getKeySet();
        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.VALUES);
        MapValueCollection result = invoke(request);
        Collection<Data> dataValues = result.getValues();
        HashSet<V> values = new HashSet<V>(dataValues.size());
        for (Data value : dataValues) {
            values.add((V) toObject(value));
        }
        return values;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.VALUES_BY_PREDICATE, predicate);
        MapValueCollection result = invoke(request);
        Collection<Data> dataValues = result.getValues();
        HashSet<V> values = new HashSet<V>(dataValues.size());
        for (Data value : dataValues) {
            values.add((V) toObject(value));
        }
        return values;
    }

    @Override
    public String getName() {
        return (String) getId();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    void onDestroy() {
    }
}
