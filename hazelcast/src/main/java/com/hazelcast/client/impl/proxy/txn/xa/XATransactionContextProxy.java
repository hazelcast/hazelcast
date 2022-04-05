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

package com.hazelcast.client.impl.proxy.txn.xa;

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.proxy.txn.ClientTxnListProxy;
import com.hazelcast.client.impl.proxy.txn.ClientTxnMapProxy;
import com.hazelcast.client.impl.proxy.txn.ClientTxnMultiMapProxy;
import com.hazelcast.client.impl.proxy.txn.ClientTxnQueueProxy;
import com.hazelcast.client.impl.proxy.txn.ClientTxnSetProxy;
import com.hazelcast.client.impl.spi.ClientTransactionContext;
import com.hazelcast.client.impl.spi.impl.ClientTransactionManagerServiceImpl;
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
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionalObjectKey;

import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides a context to perform transactional operations: beginning/committing transactions, but also retrieving
 * transactional data-structures like the {@link TransactionalMap}.
 * <p/>
 * Provides client instance and client connection proxies that need to be accessed for sending invocations.
 * <p/>
 * XA implementation of {@link ClientTransactionContext}
 */
public class XATransactionContextProxy implements ClientTransactionContext {

    final ClientTransactionManagerServiceImpl transactionManager;
    final HazelcastClientInstanceImpl client;
    final XATransactionProxy transaction;
    final ClientConnection connection;

    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap =
            new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public XATransactionContextProxy(ClientTransactionManagerServiceImpl transactionManager, Xid xid, int timeout) {
        this.transactionManager = transactionManager;
        this.client = transactionManager.getClient();
        try {
            connection = transactionManager.connect();
        } catch (Exception e) {
            throw new TransactionException("Could not obtain Connection!", e);
        }
        this.transaction = new XATransactionProxy(client, connection, xid, timeout);
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

    @Override
    public HazelcastClientInstanceImpl getClient() {
        return client;
    }

    @Override
    public ClientConnection getConnection() {
        return connection;
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

    public XATransactionProxy getTransaction() {
        return transaction;
    }
}
