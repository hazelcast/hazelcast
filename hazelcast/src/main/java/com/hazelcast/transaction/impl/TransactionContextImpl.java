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

package com.hazelcast.transaction.impl;

import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class TransactionContextImpl implements TransactionContext {

    private final NodeEngineImpl nodeEngine;
    private final TransactionImpl transaction;
    private final TransactionManagerServiceImpl transactionManager;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap
            = new HashMap<TransactionalObjectKey, TransactionalObject>(2);
    private XAResourceImpl xaResource;

    TransactionContextImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngineImpl nodeEngine,
                           TransactionOptions options, String ownerUuid) {
        this.transactionManager = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, ownerUuid);
    }

    @Override
    public XAResourceImpl getXaResource() {
        if (xaResource == null) {
            xaResource = new XAResourceImpl(transactionManager, this, nodeEngine);
        }
        return xaResource;
    }

    @Override
    public boolean isXAManaged() {
        return transaction.getXid() != null;
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
        if (transaction.getTransactionType().equals(TransactionOptions.TransactionType.TWO_PHASE)) {
            transaction.prepare();
        }
        transaction.commit();
    }

    @Override
    public void rollbackTransaction() {
        transaction.rollback();
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
            nodeEngine.getProxyService().initializeDistributedObject(serviceName, name);
            obj = ((TransactionalService) service).createTransactionalObject(name, transaction);
            txnObjectMap.put(key, obj);
        } else {
            if (service == null) {
                if (!nodeEngine.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                }
                throw new IllegalArgumentException("Unknown Service[" + serviceName + "]!");
            }
            throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
        }
        return obj;
    }

    Transaction getTransaction() {
        return transaction;
    }

    public boolean setTransactionTimeout(int seconds) {
        return transaction.setTimeoutMillis(TimeUnit.SECONDS.toMillis(seconds));
    }

    private static class TransactionalObjectKey {

        private final String serviceName;
        private final String name;

        TransactionalObjectKey(String serviceName, String name) {
            this.serviceName = serviceName;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TransactionalObjectKey)) {
                return false;
            }

            TransactionalObjectKey that = (TransactionalObjectKey) o;

            if (!name.equals(that.name)) {
                return false;
            }
            if (!serviceName.equals(that.serviceName)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = serviceName.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }
    }
}
