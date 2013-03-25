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

/**
 * @mdogan 2/26/13
 */
public class TransactionalMapProxy<K,V> extends TransactionalMapProxySupport implements TransactionalMap<K,V> {

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) {
        return containsKeyInternal(getService().toData(key));
    }

    public V get(Object key) {
        final Data value = getInternal(getService().toData(key));
        return (V) getService().toObject(value);
    }

    public V put(K key, V value) {
        final Data result = putInternal(getService().toData(key), getService().toData(value));
        return (V) getService().toObject(result);
    }

    @Override
    public void set(K key, V value) {

    }

    @Override
    public V putIfAbsent(K key, V value) {
        return null;
    }

    @Override
    public V replace(K key, V value) {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void delete(Object key) {

    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }
}
