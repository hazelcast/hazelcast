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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionalObjectKey;

import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class XATransactionContextImpl implements TransactionContext {

    private final NodeEngineImpl nodeEngine;
    private final XATransaction transaction;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap
            = new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public XATransactionContextImpl(NodeEngineImpl nodeEngine, Xid xid, UUID txOwnerUuid,
                                    int timeout, boolean originatedFromClient) {
        this.nodeEngine = nodeEngine;
        this.transaction = new XATransaction(nodeEngine, xid, txOwnerUuid, timeout, originatedFromClient);
    }

    @Override
    public void beginTransaction() {
        throw new UnsupportedOperationException("XA Transaction cannot be started manually!");
    }

    @Override
    public void commitTransaction() throws TransactionException {
        throw new UnsupportedOperationException("XA Transaction cannot be committed manually!");
    }

    @Override
    public void rollbackTransaction() {
        throw new UnsupportedOperationException("XA Transaction cannot be rolled back manually!");
    }

    @Override
    public UUID getTxnId() {
        return transaction.getTxnId();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return (TransactionalMap<K, V>) getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> TransactionalQueue<E> getQueue(String name) {
        return (TransactionalQueue<E>) getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return (TransactionalMultiMap<K, V>) getTransactionalObject(MultiMapService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> TransactionalList<E> getList(String name) {
        return (TransactionalList<E>) getTransactionalObject(ListService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> TransactionalSet<E> getSet(String name) {
        return (TransactionalSet<E>) getTransactionalObject(SetService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TransactionalObject getTransactionalObject(String serviceName, String name) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing "
                    + "transactional object -> " + serviceName + "[" + name + "]!");
        }
        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, name);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj != null) {
            return obj;
        }

        final Object service = nodeEngine.getService(serviceName);
        if (service instanceof TransactionalService) {
            nodeEngine.getProxyService().initializeDistributedObject(serviceName, name, transaction.getOwnerUuid());
            obj = ((TransactionalService) service).createTransactionalObject(name, transaction);
            txnObjectMap.put(key, obj);
        } else {
            throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
        }
        return obj;
    }

    XATransaction getTransaction() {
        return transaction;
    }
}
