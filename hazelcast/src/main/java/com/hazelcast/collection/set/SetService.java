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

package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.txn.TransactionalSetProxy;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.transaction.impl.TransactionSupport;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SetService extends CollectionService {

    public static final String SERVICE_NAME = "hz:impl:setService";

    private final ConcurrentMap<String, SetContainer> containerMap = new ConcurrentHashMap<String, SetContainer>();

    public SetService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public SetContainer getOrCreateContainer(String name, boolean backup) {
        SetContainer container = containerMap.get(name);
        if (container == null) {
            container = new SetContainer(name, nodeEngine);
            final SetContainer current = containerMap.putIfAbsent(name, container);
            if (current != null) {
                container = current;
            }
        }
        return container;
    }

    @Override
    public Map<String, ? extends CollectionContainer> getContainerMap() {
        return containerMap;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public DistributedObject createDistributedObject(String objectId) {
        return new SetProxyImpl(objectId, nodeEngine, this);
    }

    @Override
    public TransactionalSetProxy createTransactionalObject(String name, TransactionSupport transaction) {
        return new TransactionalSetProxy(name, transaction, nodeEngine, this);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final Map<String, CollectionContainer> migrationData = getMigrationData(event);
        return migrationData.isEmpty()
                ? null
                : new SetReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }
}
