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

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.map.MapService;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * @mdogan 2/26/13
 */
class TransactionContextImpl implements TransactionContext {

    private final NodeEngineImpl nodeEngine;

    private final TransactionImpl transaction;

    public TransactionContextImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.transaction  = new TransactionImpl(this.nodeEngine);
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

    @SuppressWarnings("unchecked")
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return (TransactionalMap<K, V>) getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    public <E> TransactionalQueue<E> getQueue(String name) {
        return (TransactionalQueue<E>) getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    @SuppressWarnings("unchecked")
    public TransactionalObject getTransactionalObject(String serviceName, Object id) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        final Object service = nodeEngine.getService(serviceName);
        if (service instanceof TransactionalService) {
            return ((TransactionalService) service).createTransactionalObject(id, transaction);
        }
        throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
    }
}
