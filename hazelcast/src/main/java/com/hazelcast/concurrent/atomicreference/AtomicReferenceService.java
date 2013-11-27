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

import com.hazelcast.concurrent.atomicreference.proxy.AtomicReferenceProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AtomicReferenceService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:atomicReferenceService";
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, AtomicReferenceWrapper> references = new ConcurrentHashMap<String, AtomicReferenceWrapper>();

    private final ConstructorFunction<String, AtomicReferenceWrapper> atomicReferenceConstructorFunction = new ConstructorFunction<String, AtomicReferenceWrapper>() {
        public AtomicReferenceWrapper createNew(String key) {
            return new AtomicReferenceWrapper();
        }
    };

    public AtomicReferenceService() {
    }

    public AtomicReferenceWrapper getReference(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(references, name, atomicReferenceConstructorFunction);
    }

    // need for testing..
    public boolean containsAtomicReference(String name) {
        return references.containsKey(name);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void reset() {
        references.clear();
    }

    public void shutdown(boolean terminate) {
        reset();
    }

    public AtomicReferenceProxy createDistributedObject(String name) {
        return new AtomicReferenceProxy(name, nodeEngine, this);
    }

    public void destroyDistributedObject(String name) {
        references.remove(name);
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }
        Map<String, Data> data = new HashMap<String, Data>();
        final int partitionId = event.getPartitionId();
        for (String name : references.keySet()) {
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name))) {
                data.put(name, references.get(name).get());
            }
        }
        return data.isEmpty() ? null : new AtomicReferenceReplicationOperation(data);
    }

    public void commitMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            removeReference(partitionMigrationEvent.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeReference(partitionMigrationEvent.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        removeReference(partitionId);
    }

    public void removeReference(int partitionId) {
        final Iterator<String> iterator = references.keySet().iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            if (nodeEngine.getPartitionService().getPartitionId(StringPartitioningStrategy.getPartitionKey(name)) == partitionId) {
                iterator.remove();
            }
        }
    }
}
