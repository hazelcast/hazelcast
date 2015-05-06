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
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapGetParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapPutParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapRemoveEntryParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalMultiMapValueCountParameters;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collection;

public class ClientTxnMultiMapProxy<K, V>
        extends ClientTxnProxy
        implements TransactionalMultiMap<K, V> {

    public ClientTxnMultiMapProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    public boolean put(K key, V value)
            throws TransactionException {

        ClientMessage request = TransactionalMultiMapPutParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    public Collection<V> get(K key) {
        ClientMessage request = TransactionalMultiMapGetParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId(), toData(key));

        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> collection = resultParameters.result;
        Collection<V> coll = new ArrayList<V>(collection.size());
        for (Data data : collection) {
            coll.add((V) toObject(data));
        }
        return coll;
    }

    public boolean remove(Object key, Object value) {
        ClientMessage request = TransactionalMultiMapRemoveEntryParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId(), toData(key), toData(value));
        ClientMessage response = invoke(request);
        return BooleanResultParameters.decode(response).result;
    }

    public Collection<V> remove(Object key) {
        ClientMessage request = TransactionalMultiMapRemoveParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> collection = resultParameters.result;
        Collection<V> coll = new ArrayList<V>(collection.size());
        for (Data data : collection) {
            coll.add((V) toObject(data));
        }
        return coll;
    }

    public int valueCount(K key) {
        ClientMessage request = TransactionalMultiMapValueCountParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId(), toData(key));
        ClientMessage response = invoke(request);
        return IntResultParameters.decode(response).result;
    }

    public int size() {
        ClientMessage request = TransactionalMultiMapSizeParameters
                .encode(getName(), getTransactionId(), ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        return IntResultParameters.decode(response).result;
    }

    public String getName() {
        return (String) getId();
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    void onDestroy() {
    }
}
