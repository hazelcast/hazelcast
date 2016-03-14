/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy.txn;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapSetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link TransactionalMap} interface.
 *
 * @param <K> key
 * @param <V> value
 */
public class ClientTxnMapProxy<K, V> extends ClientTxnProxy implements TransactionalMap<K, V> {

    public ClientTxnMapProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    @Override
    public boolean containsKey(Object key) {
        ClientMessage request = TransactionalMapContainsKeyCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return TransactionalMapContainsKeyCodec.decodeResponse(response).response;
    }

    @Override
    public V get(Object key) {
        ClientMessage request = TransactionalMapGetCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapGetCodec.decodeResponse(response).response);
    }

    @Override
    public V getForUpdate(Object key) {
        ClientMessage request = TransactionalMapGetForUpdateCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapGetForUpdateCodec.decodeResponse(response).response);
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalMapSizeCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalMapSizeCodec.decodeResponse(response).response;
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
        ClientMessage request = TransactionalMapPutCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value), timeunit.toMillis(ttl));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapPutCodec.decodeResponse(response).response);
    }

    @Override
    public void set(K key, V value) {
        ClientMessage request = TransactionalMapSetCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        ClientMessage request = TransactionalMapPutIfAbsentCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapPutIfAbsentCodec.decodeResponse(response).response);
    }

    @Override
    public V replace(K key, V value) {
        ClientMessage request = TransactionalMapReplaceCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapReplaceCodec.decodeResponse(response).response);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        ClientMessage request = TransactionalMapReplaceIfSameCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(oldValue), toData(newValue));
        ClientMessage response = invoke(request);
        return TransactionalMapReplaceIfSameCodec.decodeResponse(response).response;
    }

    @Override
    public V remove(Object key) {
        ClientMessage request = TransactionalMapRemoveCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapRemoveCodec.decodeResponse(response).response);
    }

    @Override
    public void delete(Object key) {
        ClientMessage request = TransactionalMapDeleteCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key));
        invoke(request);
    }

    @Override
    public boolean remove(Object key, Object value) {
        ClientMessage request = TransactionalMapRemoveIfSameCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return TransactionalMapRemoveIfSameCodec.decodeResponse(response).response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        ClientMessage request = TransactionalMapKeySetCodec.encodeRequest(name, getTransactionId(), ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = TransactionalMapKeySetCodec.decodeResponse(response).response;

        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, "Predicate should not be null!");
        ClientMessage request = TransactionalMapKeySetWithPredicateCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = TransactionalMapKeySetWithPredicateCodec.decodeResponse(response).response;

        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        ClientMessage request = TransactionalMapValuesCodec.encodeRequest(name, getTransactionId(), ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        List dataValues = TransactionalMapValuesCodec.decodeResponse(response).response;
        return new UnmodifiableLazyList<V>(dataValues, getSerializationService());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate predicate) {
        checkNotNull(predicate, "Predicate should not be null!");
        ClientMessage request = TransactionalMapValuesWithPredicateCodec.encodeRequest(name, getTransactionId(),
                ThreadUtil.getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataValues = TransactionalMapValuesWithPredicateCodec.decodeResponse(response).response;

        List<V> values = new ArrayList<V>(dataValues.size());
        for (Data value : dataValues) {
            values.add((V) toObject(value));
        }
        return values;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    void onDestroy() {
    }
}
