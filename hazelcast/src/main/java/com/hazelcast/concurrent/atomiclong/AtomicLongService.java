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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.config.Config;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicLongService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:atomicLongService";
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, AtomicLongWrapper> numbers = new ConcurrentHashMap<String, AtomicLongWrapper>();

    private final ConstructorFunction<String, AtomicLongWrapper> atomicLongConstructorFunction = new ConstructorFunction<String, AtomicLongWrapper>() {
        public AtomicLongWrapper createNew(String key) {
            return new AtomicLongWrapper();
        }
    };

    public AtomicLongService() {
    }

    public AtomicLongWrapper getNumber(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(numbers, name, atomicLongConstructorFunction);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void reset() {
        numbers.clear();
    }

    public void shutdown() {
        reset();
    }

    public Config getConfig() {
        return nodeEngine.getConfig();
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public AtomicLongProxy createDistributedObject(Object objectId) {
        return new AtomicLongProxy(String.valueOf(objectId), nodeEngine, this);
    }

    public void destroyDistributedObject(Object objectId) {
        numbers.remove(String.valueOf(objectId));
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }
        Map<String, Long> data = new HashMap<String, Long>();
        final int partitionId = event.getPartitionId();
        for (String name : numbers.keySet()) {
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(name)) {
                data.put(name, numbers.get(name).get());
            }
        }
        return data.isEmpty() ? null : new AtomicLongReplicationOperation(data);
    }

    public void commitMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            removeNumber(partitionMigrationEvent.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent partitionMigrationEvent) {
        if (partitionMigrationEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeNumber(partitionMigrationEvent.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        removeNumber(partitionId);
    }

    public void removeNumber(int partitionId) {
        final Iterator<String> iterator = numbers.keySet().iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            if (nodeEngine.getPartitionService().getPartitionId(name) == partitionId) {
                iterator.remove();
            }
        }
    }
}
