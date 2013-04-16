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

package com.hazelcast.transaction;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.*;
import com.hazelcast.map.MapService;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 2/26/13
 */
class TransactionContextImpl implements TransactionContext {

    private final NodeEngineImpl nodeEngine;
    private final TransactionImpl transaction;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap = new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public TransactionContextImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngineImpl nodeEngine, TransactionOptions options) {
        this.nodeEngine = nodeEngine;
        this.transaction = new TransactionImpl(transactionManagerService, nodeEngine, options);
    }

    public void beginTransaction() {
        transaction.begin();
    }

    public void commitTransaction() throws TransactionException {
        if (transaction.getTransactionType().equals(TransactionOptions.TransactionType.TWO_PHASE)) {
            transaction.prepare();
        }
        transaction.commit();
    }

    public void rollbackTransaction() {
        transaction.rollback();
    }

    @SuppressWarnings("unchecked")
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return (TransactionalMap<K, V>) getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    public <E> TransactionalQueue<E> getQueue(String name) {
        return (TransactionalQueue<E>) getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    public <K,V> TransactionalMultiMap<K,V> getMultiMap(String name) {
        return (TransactionalMultiMap<K,V>) getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP));
    }

    @SuppressWarnings("unchecked")
    public <E> TransactionalList<E> getList(String name) {
        return (TransactionalList<E>) getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST));
    }

    @SuppressWarnings("unchecked")
    public <E> TransactionalSet<E> getSet(String name) {
        return (TransactionalSet<E>) getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, name, CollectionProxyType.SET));
    }

    @SuppressWarnings("unchecked")
    public TransactionalObject getTransactionalObject(String serviceName, Object id) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing " +
                    "transactional object -> " + serviceName + "[" + id + "]!");
        }
        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, id);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj == null) {
            final Object service = nodeEngine.getService(serviceName);
            if (service instanceof TransactionalService) {
                obj = ((TransactionalService) service).createTransactionalObject(id, transaction);
                txnObjectMap.put(key, obj);
            } else {
                throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
            }
        }
        return obj;

    }

    private class TransactionalObjectKey {

        private final String serviceName;
        private final Object id;

        TransactionalObjectKey(String serviceName, Object id) {
            this.serviceName = serviceName;
            this.id = id;
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TransactionalObjectKey)) return false;

            TransactionalObjectKey that = (TransactionalObjectKey) o;

            if (!id.equals(that.id)) return false;
            if (!serviceName.equals(that.serviceName)) return false;

            return true;
        }

        public int hashCode() {
            int result = serviceName.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }
    }
}
