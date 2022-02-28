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

package com.hazelcast.transaction.impl;

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.transaction.TransactionalSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;

final class TransactionContextImpl implements TransactionContext {

    private final NodeEngineImpl nodeEngine;
    private final TransactionImpl transaction;
    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap
            = new HashMap<TransactionalObjectKey, TransactionalObject>(2);

    TransactionContextImpl(@Nonnull TransactionManagerServiceImpl transactionManagerService,
                           @Nonnull NodeEngineImpl nodeEngine,
                           @Nonnull TransactionOptions options,
                           @Nullable UUID ownerUuid,
                           boolean originatedFromClient) {
        this.nodeEngine = nodeEngine;
        this.transaction = new TransactionImpl(transactionManagerService, nodeEngine, options, ownerUuid, originatedFromClient);
    }

    @Override
    public UUID getTxnId() {
        return transaction.getTxnId();
    }

    @Override
    public void beginTransaction() {
        transaction.begin();
    }

    @Override
    public void commitTransaction() throws TransactionException {
        if (transaction.requiresPrepare()) {
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
    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return (TransactionalMultiMap<K, V>) getTransactionalObject(MultiMapService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> TransactionalQueue<E> getQueue(String name) {
        return (TransactionalQueue<E>) getTransactionalObject(QueueService.SERVICE_NAME, name);
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
        checkActive(serviceName, name);

        if (requiresBackupLogs(serviceName)) {
            transaction.ensureBackupLogsExist();
        }

        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, name);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj != null) {
            return obj;
        }

        TransactionalService transactionalService = getTransactionalService(serviceName);
        nodeEngine.getProxyService().initializeDistributedObject(serviceName, name, transaction.getOwnerUuid());
        obj = transactionalService.createTransactionalObject(name, transaction);
        txnObjectMap.put(key, obj);
        return obj;
    }

    private boolean requiresBackupLogs(String serviceName) {
        if (serviceName.equals(MapService.SERVICE_NAME)) {
            return false;
        }

        if (serviceName.equals(MultiMapService.SERVICE_NAME)) {
            return false;
        }

        return true;
    }

    private TransactionalService getTransactionalService(String serviceName) {
        final Object service = nodeEngine.getService(serviceName);
        if (!(service instanceof TransactionalService)) {
            throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
        }

        return (TransactionalService) service;
    }

    private void checkActive(String serviceName, String name) {
        if (transaction.getState() != ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing "
                    + "transactional object -> " + serviceName + "[" + name + "]!");
        }
    }

    Transaction getTransaction() {
        return transaction;
    }
}
