/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy.txn;

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
import com.hazelcast.client.impl.spi.ClientTransactionContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.transaction.TransactionalMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;

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
        checkNotNull(key, "key can't be null");

        ClientMessage request = TransactionalMapContainsKeyCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return TransactionalMapContainsKeyCodec.decodeResponse(response);
    }

    @Override
    public V get(Object key) {
        checkNotNull(key, "key can't be null");

        ClientMessage request = TransactionalMapGetCodec.encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapGetCodec.decodeResponse(response));
    }

    @Override
    public V getForUpdate(Object key) {
        checkNotNull(key, "key can't be null");

        ClientMessage request = TransactionalMapGetForUpdateCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapGetForUpdateCodec.decodeResponse(response));
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalMapSizeCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalMapSizeCodec.decodeResponse(response);
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
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        ClientMessage request = TransactionalMapPutCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value), timeunit.toMillis(ttl));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapPutCodec.decodeResponse(response));
    }

    @Override
    public void set(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        ClientMessage request = TransactionalMapSetCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        ClientMessage request = TransactionalMapPutIfAbsentCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapPutIfAbsentCodec.decodeResponse(response));
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        ClientMessage request = TransactionalMapReplaceCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapReplaceCodec.decodeResponse(response));
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key, "key can't be null");
        checkNotNull(oldValue, "oldValue can't be null");
        checkNotNull(newValue, "newValue can't be null");

        ClientMessage request = TransactionalMapReplaceIfSameCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(oldValue), toData(newValue));
        ClientMessage response = invoke(request);
        return TransactionalMapReplaceIfSameCodec.decodeResponse(response);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, "key can't be null");

        ClientMessage request = TransactionalMapRemoveCodec.encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return (V) toObject(TransactionalMapRemoveCodec.decodeResponse(response));
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, "key can't be null");

        ClientMessage request = TransactionalMapDeleteCodec.encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        invoke(request);
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        ClientMessage request = TransactionalMapRemoveIfSameCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return TransactionalMapRemoveIfSameCodec.decodeResponse(response);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        ClientMessage request = TransactionalMapKeySetCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = TransactionalMapKeySetCodec.decodeResponse(response);

        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, "predicate can't null");

        ClientMessage request = TransactionalMapKeySetWithPredicateCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataKeySet = TransactionalMapKeySetWithPredicateCodec.decodeResponse(response);

        HashSet<K> keySet = new HashSet<K>(dataKeySet.size());
        for (Data data : dataKeySet) {
            keySet.add((K) toObject(data));
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        ClientMessage request = TransactionalMapValuesCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        List dataValues = TransactionalMapValuesCodec.decodeResponse(response);
        return new UnmodifiableLazyList(dataValues, getSerializationService());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate predicate) {
        checkNotNull(predicate, "predicate can't be null");

        ClientMessage request = TransactionalMapValuesWithPredicateCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(predicate));
        ClientMessage response = invoke(request);
        Collection<Data> dataValues = TransactionalMapValuesWithPredicateCodec.decodeResponse(response);

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
