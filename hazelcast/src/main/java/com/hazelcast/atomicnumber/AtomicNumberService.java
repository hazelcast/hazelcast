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

package com.hazelcast.atomicnumber;

import com.hazelcast.atomicnumber.proxy.AtomicNumberProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.spi.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicNumberService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String NAME = "hz:impl:atomicNumberService";

    private NodeEngine nodeEngine;

    private final ConcurrentMap<String, Long> numbers = new ConcurrentHashMap<String, Long>();

    public AtomicNumberService() {
    }

    public long getNumber(String name) {
        Long value = numbers.get(name);
        if (value == null) {
            value = 0L;
            numbers.put(name, value);
        }
        return value;
    }

    public void setNumber(String name, long newValue) {
        numbers.put(name, newValue);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {
        numbers.clear();
    }

    public Config getConfig() {
        return nodeEngine.getConfig();
    }

    public String getServiceName() {
        return NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new AtomicNumberProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return createDistributedObject(objectId);
    }

    public void destroyDistributedObject(Object objectId) {
        numbers.remove(String.valueOf(objectId));
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent migrationServiceEvent) {
        Map<String, Long> data = new HashMap<String, Long>();
        final int partitionId = migrationServiceEvent.getPartitionId();
        for (String name : numbers.keySet()) {
            if (partitionId == nodeEngine.getPartitionId(name)) {
                data.put(name, numbers.get(name));
            }
        }
        return data.isEmpty() ? null : new AtomicNumberMigrationOperation(data);
    }

    public void commitMigration(MigrationServiceEvent migrationServiceEvent) {
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
        if (migrationServiceEvent.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeNumber(migrationServiceEvent.getPartitionId());
        }
    }

    public void removeNumber(int partitionId) {
        final Iterator<String> iter = numbers.keySet().iterator();
        while (iter.hasNext()) {
            String name = iter.next();
            if (nodeEngine.getPartitionId(name) == partitionId) {
                iter.remove();
            }
        }
    }

    public int getMaxBackupCount() {
        return 1;
    }
}
