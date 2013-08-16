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
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.operations.client.*;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.transaction.TransactionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * @author ali 6/10/13
 */
public class ClientTxnMultiMapProxy<K, V> extends ClientTxnProxy implements TransactionalMultiMap<K,V> {

    public ClientTxnMultiMapProxy(CollectionProxyId id, TransactionContextProxy proxy) {
        super(id, proxy);
    }

    public boolean put(K key, V value) throws TransactionException {
        TxnMultiMapPutRequest request = new TxnMultiMapPutRequest(getName(), toData(key), toData(value));
        final Boolean result = invoke(request);
        return result;
    }

    public Collection<V> get(K key) {
        TxnMultiMapGetRequest request = new TxnMultiMapGetRequest(getName(), toData(key));
        final PortableCollection portableCollection = invoke(request);
        final Collection<Data> collection = portableCollection.getCollection();
        Collection<V> coll;
        if (collection instanceof List){
            coll = new ArrayList<V>(collection.size());
        } else {
            coll = new HashSet<V>(collection.size());
        }
        for (Data data : collection) {
            coll.add((V)toObject(data));
        }
        return coll;
    }

    public boolean remove(Object key, Object value) {
        TxnMultiMapRemoveRequest request = new TxnMultiMapRemoveRequest(getName(), toData(key), toData(value));
        Boolean result = invoke(request);
        return result;
    }

    public Collection<V> remove(Object key) {
        TxnMultiMapRemoveRequest request = new TxnMultiMapRemoveRequest(getName(), toData(key));
        PortableCollection portableCollection = invoke(request);
        final Collection<Data> collection = portableCollection.getCollection();
        Collection<V> coll;
        if (collection instanceof List){
            coll = new ArrayList<V>(collection.size());
        } else {
            coll = new HashSet<V>(collection.size());
        }
        for (Data data : collection) {
            coll.add((V)toObject(data));
        }
        return coll;
    }

    public int valueCount(K key) {
        TxnMultiMapValueCountRequest request = new TxnMultiMapValueCountRequest(getName(), toData(key));
        Integer result = invoke(request);
        return result;
    }

    public int size() {
        TxnMultiMapSizeRequest request = new TxnMultiMapSizeRequest(getName());
        Integer result = invoke(request);
        return result;
    }

    public String getName() {
        final CollectionProxyId proxyId = (CollectionProxyId)getId();
        return proxyId.getName();
    }

    @Override
    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    void onDestroy() {
        //TODO
    }
}
