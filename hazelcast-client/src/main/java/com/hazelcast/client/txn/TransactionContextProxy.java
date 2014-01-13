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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.txn.proxy.*;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.*;
import com.hazelcast.map.MapService;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.queue.QueueService;
import com.hazelcast.transaction.*;
import com.hazelcast.transaction.impl.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ali 6/6/13
 */
public class TransactionContextProxy implements TransactionContext {

    final HazelcastClient client;
    final TransactionProxy transaction;
    final ClientConnection connection;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap = new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public TransactionContextProxy(HazelcastClient client, TransactionOptions options) {
        this.client = client;
        this.connection = connect();
        this.transaction = new TransactionProxy(client, options, connection);
    }

    public String getTxnId() {
        return transaction.getTxnId();
    }

    public void beginTransaction() {
        transaction.begin();
    }

    public void commitTransaction() throws TransactionException {
        transaction.commit();
    }

    public void rollbackTransaction() {
        transaction.rollback();
    }

    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    public <E> TransactionalQueue<E> getQueue(String name) {
        return getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return getTransactionalObject(MultiMapService.SERVICE_NAME, name);
    }

    public <E> TransactionalList<E> getList(String name) {
        return getTransactionalObject(ListService.SERVICE_NAME, name);
    }

    public <E> TransactionalSet<E> getSet(String name) {
        return getTransactionalObject(SetService.SERVICE_NAME, name);
    }

    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing " +
                    "transactional object -> " + serviceName + "[" + name + "]!");
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
            }else if (serviceName.equals(SetService.SERVICE_NAME)) {
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

    public HazelcastClient getClient() {
        return client;
    }

    private ClientConnection connect() {
        int count = 0;
        final ClientConnectionManager connectionManager = client.getConnectionManager();
        IOException lastError = null;
        while (count < ClientClusterServiceImpl.RETRY_COUNT) {
            try {
                return connectionManager.getRandomConnection();
            } catch (IOException e) {
                lastError = e;
            }
            count++;
        }
        throw new HazelcastException("Could not obtain Connection!!!", lastError);
    }

    private static class TransactionalObjectKey {

        private final String serviceName;
        private final String name;

        TransactionalObjectKey(String serviceName, String name) {
            this.serviceName = serviceName;
            this.name = name;
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TransactionalObjectKey)) return false;

            TransactionalObjectKey that = (TransactionalObjectKey) o;

            if (!name.equals(that.name)) return false;
            if (!serviceName.equals(that.serviceName)) return false;

            return true;
        }

        public int hashCode() {
            int result = serviceName.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }
    }
}
