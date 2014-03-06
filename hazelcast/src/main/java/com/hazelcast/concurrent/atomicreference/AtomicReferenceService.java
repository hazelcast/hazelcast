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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AtomicReferenceReplicationOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class AtomicReferenceService implements ManagedService, RemoteService, MigrationAwareService {

    /**
     * The name of the AtomicReferenceService.
     */
    public static final String SERVICE_NAME = "hz:impl:atomicReferenceService";

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, ReferenceWrapper> references = new ConcurrentHashMap<String, ReferenceWrapper>();
    private final ConstructorFunction<String, ReferenceWrapper> atomicReferenceConstructorFunction =
            new ConstructorFunction<String, ReferenceWrapper>() {
                public ReferenceWrapper createNew(String key) {
                    return new ReferenceWrapper();
                }
            };

    public AtomicReferenceService() {
    }

    public ReferenceWrapper getReference(String name) {
        return getOrPutIfAbsent(references, name, atomicReferenceConstructorFunction);
    }

    // need for testing..
    public boolean containsAtomicReference(String name) {
        return references.containsKey(name);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        references.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public AtomicReferenceProxy createDistributedObject(String name) {
        return new AtomicReferenceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        references.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Data> data = new HashMap<String, Data>();
        int partitionId = event.getPartitionId();
        for (String name : references.keySet()) {
            if (partitionId == getPartitionId(name)) {
                ReferenceWrapper referenceWrapper = references.get(name);
                Data value = referenceWrapper.get();
                data.put(name, value);
            }
        }
        return data.isEmpty() ? null : new AtomicReferenceReplicationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int partitionId = partitionMigrationEvent.getPartitionId();
            removeReference(partitionId);
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int partitionId = partitionMigrationEvent.getPartitionId();
            removeReference(partitionId);
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        removeReference(partitionId);
    }

    public void removeReference(int partitionId) {
        final Iterator<String> iterator = references.keySet().iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            if (getPartitionId(name) == partitionId) {
                iterator.remove();
            }
        }
    }

    private int getPartitionId(String name) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
    }
}
