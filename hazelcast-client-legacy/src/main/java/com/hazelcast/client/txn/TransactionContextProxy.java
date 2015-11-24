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

package com.hazelcast.client.txn;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientTransactionContext;
import com.hazelcast.client.spi.impl.ClientTransactionManagerServiceImpl;
import com.hazelcast.client.txn.proxy.ClientTxnListProxy;
import com.hazelcast.client.txn.proxy.ClientTxnMapProxy;
import com.hazelcast.client.txn.proxy.ClientTxnMultiMapProxy;
import com.hazelcast.client.txn.proxy.ClientTxnQueueProxy;
import com.hazelcast.client.txn.proxy.ClientTxnSetProxy;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionalObjectKey;

import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.Map;

public class TransactionContextProxy implements ClientTransactionContext {

    final ClientTransactionManagerServiceImpl transactionManager;
    final HazelcastClientInstanceImpl client;
    final TransactionProxy transaction;
    final ClientConnection connection;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap =
            new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public TransactionContextProxy(ClientTransactionManagerServiceImpl transactionManager, TransactionOptions options) {
        this.transactionManager = transactionManager;
        this.client = transactionManager.getClient();
        try {
            connection = transactionManager.connect();
        } catch (Exception e) {
            throw new HazelcastException("Could not obtain Connection!!!", e);
        }
        this.transaction = new TransactionProxy(client, options, connection);
    }

    @Override
    public String getTxnId() {
        return transaction.getTxnId();
    }

    @Override
    public void beginTransaction() {
        transaction.begin();
    }

    @Override
    public void commitTransaction() throws TransactionException {
        transaction.commit();
    }

    @Override
    public void rollbackTransaction() {
        transaction.rollback();
    }

    @Override
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalQueue<E> getQueue(String name) {
        return getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return getTransactionalObject(MultiMapService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalList<E> getList(String name) {
        return getTransactionalObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalSet<E> getSet(String name) {
        return getTransactionalObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing "
                    + "transactional object -> " + serviceName + "[" + name + "]!");
        }
        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, name);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj == null) {
            if (serviceName.equals(QueueService.SERVICE_NAME)) {
                obj = new ClientTxnQueueProxy(name, this);
            } else if (serviceName.equals(MapService.SERVICE_NAME)) {
                obj = new ClientTxnMapProxy(name, this);
            } else if (serviceName.equals(MultiMapService.SERVICE_NAME)) {
                obj = new ClientTxnMultiMapProxy(name, this);
            } else if (serviceName.equals(ListService.SERVICE_NAME)) {
                obj = new ClientTxnListProxy(name, this);
            } else if (serviceName.equals(SetService.SERVICE_NAME)) {
                obj = new ClientTxnSetProxy(name, this);
            }

            if (obj == null) {
                throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
            }
            txnObjectMap.put(key, obj);
        }
        return (T) obj;
    }

    public ClientConnection getConnection() {
        return connection;
    }

    public HazelcastClientInstanceImpl getClient() {
        return client;
    }

    @Override
    public XAResource getXaResource() {
        return client.getXAResource();
    }
}
