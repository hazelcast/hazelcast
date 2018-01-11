/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.transaction.TransactionException;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.util.ThreadUtil.getThreadId;

/**
 * Proxy implementation of {@link TransactionalMultiMap}
 *
 * @param <K> key
 * @param <V> value
 */
public class ClientTxnMultiMapProxy<K, V> extends ClientTxnProxy implements TransactionalMultiMap<K, V> {

    public ClientTxnMultiMapProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    @Override
    public boolean put(K key, V value) throws TransactionException {
        ClientMessage request = TransactionalMultiMapPutCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return TransactionalMultiMapPutCodec.decodeResponse(response).response;
    }

    @Override
    public Collection<V> get(K key) {
        ClientMessage request = TransactionalMultiMapGetCodec.encodeRequest(name, getTransactionId(), getThreadId(), toData(key));

        ClientMessage response = invoke(request);
        List<Data> collection = TransactionalMultiMapGetCodec.decodeResponse(response).response;
        return new UnmodifiableLazyList<V>(collection, getSerializationService());
    }

    @Override
    public boolean remove(Object key, Object value) {
        ClientMessage request = TransactionalMultiMapRemoveEntryCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return TransactionalMultiMapRemoveEntryCodec.decodeResponse(response).response;
    }

    @Override
    public Collection<V> remove(Object key) {
        ClientMessage request = TransactionalMultiMapRemoveCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        List<Data> collection = TransactionalMultiMapRemoveCodec.decodeResponse(response).response;
        return new UnmodifiableLazyList<V>(collection, getSerializationService());
    }

    @Override
    public int valueCount(K key) {
        ClientMessage request = TransactionalMultiMapValueCountCodec
                .encodeRequest(name, getTransactionId(), getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return TransactionalMultiMapValueCountCodec.decodeResponse(response).response;
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalMultiMapSizeCodec
                .encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalMultiMapSizeCodec.decodeResponse(response).response;
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    void onDestroy() {
    }
}
