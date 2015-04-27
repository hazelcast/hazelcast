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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapContainsKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapDeleteParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapGetForUpdateParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapGetParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapKeySetParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapKeySetWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapPutIfAbsentParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapPutParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapRemoveIfSameParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapReplaceIfSameParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapReplaceParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapSetParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapValuesParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMapValuesWithPredicateParameters;
import com.hazelcast.client.txn.TransactionContextProxy;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.ThreadUtil;

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
        ClientMessage request = TransactionalMapContainsKeyParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    @Override
    public V get(Object key) {
        ClientMessage request = TransactionalMapGetParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public V getForUpdate(Object key) {
        ClientMessage request = TransactionalMapGetForUpdateParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalMapSizeParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return IntResultParameters.decode(response).result;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        ClientMessage request = TransactionalMapPutParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value), timeunit.toMillis(ttl));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public void set(K key, V value) {
        ClientMessage request = TransactionalMapSetParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        ClientMessage request = TransactionalMapPutIfAbsentParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public V replace(K key, V value) {
        ClientMessage request = TransactionalMapReplaceParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        ClientMessage request = TransactionalMapReplaceIfSameParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(oldValue), toData(newValue));
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    @Override
    public V remove(Object key) {
        ClientMessage request = TransactionalMapRemoveParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(GenericResultParameters.decode(response).result);
    }

    @Override
    public void delete(Object key) {
        ClientMessage request = TransactionalMapDeleteParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        invoke(request);
    }

    @Override
    public boolean remove(Object key, Object value) {
        ClientMessage request = TransactionalMapRemoveIfSameParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        ClientMessage request = TransactionalMapKeySetParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = DataCollectionResultParameters.decode(response).result;
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
        ClientMessage request = TransactionalMapKeySetWithPredicateParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = DataCollectionResultParameters.decode(response).result;

        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        ClientMessage request = TransactionalMapValuesParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        Collection<Data> dataValues = DataCollectionResultParameters.decode(response).result;
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
        ClientMessage request = TransactionalMapValuesWithPredicateParameters.encode(getName(), getTransactionId(),
                ThreadUtil.getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataValues = DataCollectionResultParameters.decode(response).result;

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
