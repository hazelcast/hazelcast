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

import com.hazelcast.core.Transaction;
import com.hazelcast.core.TransactionContext;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.core.TransactionalObject;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.map.MapService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * @mdogan 2/26/13
 */
public class TransactionCtxImpl implements TransactionContext {

    private final HazelcastInstanceImpl hazelcastInstance;
    private final NodeEngineImpl nodeEngine;

    private final TransactionImpl transaction;

    public TransactionCtxImpl(HazelcastInstanceImpl hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.nodeEngine = hazelcastInstance.node.nodeEngine;
        this.transaction  = new TransactionImpl(nodeEngine);
    }

    public Transaction beginTransaction() {
        transaction.begin();
        return transaction;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void commitTransaction() throws TransactionException {
        transaction.commit();
    }

    public void rollbackTransaction() {
        transaction.rollback();
    }

    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    public <T extends TransactionalObject> T getDistributedObject(String serviceName, Object id) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active!");
        }
        final TransactionalService service = nodeEngine.getService(serviceName);
        return service.createTransactionalObject(id, transaction);
    }
}
