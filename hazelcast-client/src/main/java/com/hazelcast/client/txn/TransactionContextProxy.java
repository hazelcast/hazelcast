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
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.txn.proxy.*;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.*;
import com.hazelcast.map.MapService;
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

    final int CONNECTION_TRY_COUNT = 5;
    final HazelcastClient client;
    final TransactionProxy transaction;
    final Connection connection;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap = new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    public TransactionContextProxy(HazelcastClient client, TransactionOptions options) {
        this.client = client;
        this.connection = connect();
        if (connection == null) {
            throw new HazelcastException("Could not obtain Connection!!!");
        }
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
        return getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP));
    }

    public <E> TransactionalList<E> getList(String name) {
        return getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST));
    }

    public <E> TransactionalSet<E> getSet(String name) {
        return getTransactionalObject(CollectionService.SERVICE_NAME, new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, name, CollectionProxyType.SET));
    }

    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, Object id) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing " +
                    "transactional object -> " + serviceName + "[" + id + "]!");
        }
        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, id);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj == null) {
            if (serviceName.equals(QueueService.SERVICE_NAME)) {
                obj = new ClientTxnQueueProxy(String.valueOf(id), this);
            } else if (serviceName.equals(MapService.SERVICE_NAME)) {
                obj = new ClientTxnMapProxy(String.valueOf(id), this);
            } else if (serviceName.equals(CollectionService.SERVICE_NAME)) {
                CollectionProxyId proxyId = (CollectionProxyId) id;
                if (proxyId.getType().equals(CollectionProxyType.MULTI_MAP)) {
                    obj = new ClientTxnMultiMapProxy(proxyId, this);
                } else if (proxyId.getType().equals(CollectionProxyType.LIST)) {
                    obj = new ClientTxnListProxy(proxyId, this);
                } else if (proxyId.getType().equals(CollectionProxyType.SET)) {
                    obj = new ClientTxnSetProxy(proxyId, this);
                }
            }
            if (obj == null) {
                throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
            }
            txnObjectMap.put(key, obj);
        }
        return (T) obj;
    }

    public Connection getConnection() {
        return connection;
    }

    public HazelcastClient getClient() {
        return client;
    }

    private Connection connect() {
        Connection conn = null;
        for (int i = 0; i < CONNECTION_TRY_COUNT; i++) {
            try {
                conn = client.getConnectionManager().getRandomConnection();
            } catch (IOException e) {
                continue;
            }
            if (conn != null) {
                break;
            }
        }
        return conn;
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
