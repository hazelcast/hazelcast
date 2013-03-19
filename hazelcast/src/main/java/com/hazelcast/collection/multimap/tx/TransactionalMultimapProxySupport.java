/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalObject;

/**
 * @ali 3/13/13
 */
public abstract class TransactionalMultimapProxySupport<E> extends AbstractDistributedObject<CollectionService> implements TransactionalObject{

    protected final CollectionProxyId proxyId;
    protected final Transaction tx;

    protected TransactionalMultimapProxySupport(CollectionProxyId proxyId, NodeEngine nodeEngine, CollectionService service, Transaction tx) {
        super(nodeEngine, service);
        this.proxyId = proxyId;
        this.tx = tx;
    }

    protected boolean putInternal(Data key, Data value) throws TransactionException {
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
        tx.addPartition(partitionId);
        return false;
    }

    public Object getId() {
        return proxyId;
    }

    public String getName() {
        return proxyId.getName();
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }
}
