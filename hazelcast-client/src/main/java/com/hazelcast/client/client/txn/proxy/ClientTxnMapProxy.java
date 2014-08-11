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

package com.hazelcast.client.client.txn.proxy;

import com.hazelcast.client.client.txn.TransactionContextProxy;
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
import java.util.concurrent.TimeUnit;

/**
 * @author ali 6/10/13
 */
public class ClientTxnMapProxy<K, V> extends ClientTxnProxy implements TransactionalMap<K, V> {

    public ClientTxnMapProxy(String name, TransactionContextProxy proxy) {
        super(name, proxy);
    }

    public boolean containsKey(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.CONTAINS_KEY, toData(key));
        Boolean result = invoke(request);
        return result;
    }

    public V get(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.GET, toData(key));
        return invoke(request);
    }

    public V getForUpdate(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.GET_FOR_UPDATE, toData(key));
        return invoke(request);
    }

    public int size() {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.SIZE);
        Integer result = invoke(request);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public V put(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(), TxnMapRequest.TxnMapRequestType.PUT, toData(key), toData(value));
        return invoke(request);
    }

    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.PUT_WITH_TTL, toData(key), toData(value), ttl, timeunit);
        return invoke(request);
    }

    public void set(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.SET, toData(key), toData(value));
        invoke(request);
    }

    public V putIfAbsent(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.PUT_IF_ABSENT, toData(key), toData(value));
        return invoke(request);
    }

    public V replace(K key, V value) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.REPLACE, toData(key), toData(value));
        return invoke(request);
    }

    public boolean replace(K key, V oldValue, V newValue) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.REPLACE_IF_SAME, toData(key), toData(oldValue), toData(newValue));
        Boolean result = invoke(request);
        return result;
    }

    public V remove(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.REMOVE, toData(key));
        return invoke(request);
    }

    public void delete(Object key) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.DELETE, toData(key));
        invoke(request);
    }

    public boolean remove(Object key, Object value) {
        TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.REMOVE_IF_SAME, toData(key), toData(value));
        Boolean result = invoke(request);
        return result;
    }

    public Set<K> keySet() {
        final TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.KEYSET);
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
        final TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.KEYSET_BY_PREDICATE, predicate);
        final MapKeySet result = invoke(request);
        final Set<Data> dataKeySet = result.getKeySet();
        final HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    public Collection<V> values() {
        final TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.VALUES);
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
        final TxnMapRequest request = new TxnMapRequest(getName(),
                TxnMapRequest.TxnMapRequestType.VALUES_BY_PREDICATE, predicate);
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
