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

package com.hazelcast.map.tx;

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;

/**
 * @mdogan 2/26/13
 */
public class TransactionalMapProxy<K,V> extends TransactionalMapProxySupport implements TransactionalMap<K,V> {

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        return containsKeyInternal(nodeEngine.toData(key));
    }

    public V get(Object key) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data value = getInternal(nodeEngine.toData(key));
        return nodeEngine.toObject(value);
    }

    public V put(K key, V value) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data result = putInternal(nodeEngine.toData(key), nodeEngine.toData(value));
        return nodeEngine.toObject(result);
    }

    public void set(K key, V value) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        setInternal(nodeEngine.toData(key), nodeEngine.toData(value));
    }

    public V putIfAbsent(K key, V value) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data result = putIfAbsentInternal(nodeEngine.toData(key), nodeEngine.toData(value));
        return nodeEngine.toObject(result);
    }

    public V replace(K key, V value) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data result = replaceInternal(nodeEngine.toData(key), nodeEngine.toData(value));
        return nodeEngine.toObject(result);
    }

    public boolean replace(K key, V testValue, V newValue) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        return replaceInternal(nodeEngine.toData(key), nodeEngine.toData(testValue), nodeEngine.toData(newValue));
    }

    public V remove(Object key) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data result = removeInternal(nodeEngine.toData(key));
        return nodeEngine.toObject(result);
    }

    public void delete(Object key) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        deleteInternal(nodeEngine.toData(key));
    }

    public boolean remove(Object key, Object value) throws TransactionException {
        final NodeEngine nodeEngine = getNodeEngine();
        return removeInternal(nodeEngine.toData(key), nodeEngine.toData(value));
    }
}
