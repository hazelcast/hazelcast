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

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.concurrent.atomiclong.client.GetAndSetHandler;
import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.config.Config;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

// author: sancar - 21.12.2012
public class AtomicLongService implements ManagedService, RemoteService, MigrationAwareService, ClientProtocolService {

    public static final String SERVICE_NAME = "hz:impl:atomicLongService";
    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, AtomicLong> numbers = new ConcurrentHashMap<String, AtomicLong>();

    private final ConcurrencyUtil.ConstructorFunction<String, AtomicLong> atomicLongConstructorFunction = new ConcurrencyUtil.ConstructorFunction<String, AtomicLong>() {
        public AtomicLong createNew(String key) {
            return new AtomicLong(0L);
        }
    };

    public AtomicLongService() {
    }

    public AtomicLong getNumber(String name) {
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

    public AtomicLongProxy createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {
        numbers.remove(String.valueOf(objectId));
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent migrationServiceEvent) {
        if (migrationServiceEvent.getReplicaIndex() > 1) {
            return null;
        }
        Map<String, Long> data = new HashMap<String, Long>();
        final int partitionId = migrationServiceEvent.getPartitionId();
        for (String name : numbers.keySet()) {
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(name)) {
                data.put(name, numbers.get(name).get());
            }
        }
        return data.isEmpty() ? null : new AtomicLongMigrationOperation(data);
    }

    public void commitMigration(MigrationServiceEvent migrationServiceEvent) {
        if (migrationServiceEvent.getReplicaIndex() > 1) {
            return;
        }
        if (migrationServiceEvent.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            if (migrationServiceEvent.getMigrationType() == MigrationType.MOVE) {
                removeNumber(migrationServiceEvent.getPartitionId());
            }
        } else if (migrationServiceEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {

        } else {
            throw new IllegalStateException("Nor source neither destination, probably bug");
        }
    }

    public void rollbackMigration(MigrationServiceEvent migrationServiceEvent) {
        if (migrationServiceEvent.getReplicaIndex() > 1) {
            return;
        }
        if (migrationServiceEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeNumber(migrationServiceEvent.getPartitionId());
        }
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

    @Override
    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();
        commandHandlers.put(Command.ALADDANDGET, new GetAndSetHandler(this));
        commandHandlers.put(Command.ALGETANDADD, new GetAndSetHandler(this));
        commandHandlers.put(Command.ALGETANDSET, new GetAndSetHandler(this));
//        commandHandlers.put(Command.ALCOMPAREANDSET, new CompareAndSetHandler(this));
        return commandHandlers ;
    }


}
