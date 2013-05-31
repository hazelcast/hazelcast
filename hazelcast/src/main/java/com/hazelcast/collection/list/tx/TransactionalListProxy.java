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

package com.hazelcast.collection.list.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.multimap.tx.TransactionalMultiMapProxySupport;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;

/**
 * @ali 4/16/13
 */
public class TransactionalListProxy<E> extends TransactionalMultiMapProxySupport implements TransactionalList<E> {

    final Data key;

    public TransactionalListProxy(NodeEngine nodeEngine, CollectionService service, CollectionProxyId proxyId, Transaction tx) {
        super(nodeEngine, service, proxyId, tx,
                nodeEngine.getConfig().getMultiMapConfig("list:" + proxyId.getKeyName()).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST));
        this.key = nodeEngine.toData(proxyId.getKeyName());
    }

    public boolean add(E e) {
        Data value = getNodeEngine().toData(e);
        return putInternal(key, value);
    }

    public boolean remove(E e) {
        Data value = getNodeEngine().toData(e);
        return removeInternal(key, value);
    }

    public int size() {
        return valueCountInternal(key);
    }

}
