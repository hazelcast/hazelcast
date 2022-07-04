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

package com.hazelcast.internal.partition.service;

import com.hazelcast.config.ServiceConfig;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.partition.MigrationEndpoint;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertNull;

public class TestMigrationAwareService extends TestAbstractMigrationAwareService<Void> {

    public static final String SERVICE_NAME = TestMigrationAwareService.class.getSimpleName();

    public static ServiceConfig createServiceConfig(int backupCount) {
        return new ServiceConfig()
                .setEnabled(true).setName(TestMigrationAwareService.SERVICE_NAME)
                .setClassName(TestMigrationAwareService.class.getName())
                .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));
    }

    private final ConcurrentMap<Integer, Integer> data = new ConcurrentHashMap<>();

    int inc(int partitionId) {
        Integer count = data.get(partitionId);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        data.put(partitionId, count);
        return count;
    }

    void put(int partitionId, int value) {
        data.put(partitionId, value);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > backupCount) {
            return null;
        }
        if (!data.containsKey(event.getPartitionId())) {
            return null;
        }
        return new TestReplicationOperation(data.get(event.getPartitionId()));
    }

    @Override
    protected void onCommitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > backupCount) {
                data.remove(event.getPartitionId());
            }
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            if (event.getNewReplicaIndex() > backupCount) {
                assertNull(data.get(event.getPartitionId()));
            }
        }
    }

    @Override
    protected void onRollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > backupCount) {
                data.remove(event.getPartitionId());
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        data.remove(partitionId);
    }

    public int size() {
        return data.size();
    }

    public Integer get(int id) {
        return data.get(id);
    }

    @Override
    public int size(Void name) {
        return data.size();
    }

    @Override
    public Integer get(Void name, int id) {
        return data.get(id);
    }

    @Override
    public Collection<Integer> keys(Void name) {
        return data.keySet();
    }

    public boolean contains(int partitionId) {
        return data.containsKey(partitionId);
    }

    @Override
    public boolean contains(Void name, int partitionId) {
        return data.containsKey(partitionId);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public ServiceNamespace getNamespace(Void name) {
        return NonFragmentedServiceNamespace.INSTANCE;
    }
}
