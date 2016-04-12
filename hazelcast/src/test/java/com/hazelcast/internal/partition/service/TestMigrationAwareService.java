/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertNull;

/**
 */
public class TestMigrationAwareService implements ManagedService, MigrationAwareService {

    public static final String SERVICE_NAME = MigrationAwareService.class.getSimpleName();

    private static final String BACKUP_COUNT_PROP = "backups.count";

    public static ServiceConfig createServiceConfig(int backupCount) {
        return new ServiceConfig()
                .setEnabled(true).setName(TestMigrationAwareService.SERVICE_NAME)
                .setClassName(TestMigrationAwareService.class.getName())
                .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));
    }

    private final ConcurrentMap<Integer, Integer> data = new ConcurrentHashMap<Integer, Integer>();

    private final List<PartitionMigrationEvent> beforeEvents = new ArrayList<PartitionMigrationEvent>();

    private final List<PartitionMigrationEvent> commitEvents = new ArrayList<PartitionMigrationEvent>();

    private final List<PartitionMigrationEvent> rollbackEvents = new ArrayList<PartitionMigrationEvent>();

    volatile int backupCount;

    private volatile ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        backupCount = Integer.parseInt(properties.getProperty(BACKUP_COUNT_PROP, "1"));
        logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

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
    public void beforeMigration(PartitionMigrationEvent event) {
        synchronized (beforeEvents) {
            beforeEvents.add(event);
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > backupCount) {
            return null;
        }
        if (!data.containsKey(event.getPartitionId())) {
            throw new HazelcastException("No data found for " + event);
        }
        return new TestReplicationOperation(data.get(event.getPartitionId()));
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        synchronized (commitEvents) {
            commitEvents.add(event);
        }

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
    public void rollbackMigration(PartitionMigrationEvent event) {
        synchronized (rollbackEvents) {
            rollbackEvents.add(event);
        }

        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > backupCount) {
                data.remove(event.getPartitionId());
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        data.remove(partitionId);
    }

    public List<PartitionMigrationEvent> getBeforeEvents() {
        synchronized (beforeEvents) {
            return new ArrayList<PartitionMigrationEvent>(beforeEvents);
        }
    }

    public List<PartitionMigrationEvent> getCommitEvents() {
        synchronized (commitEvents) {
            return new ArrayList<PartitionMigrationEvent>(commitEvents);
        }
    }

    public List<PartitionMigrationEvent> getRollbackEvents() {
        synchronized (rollbackEvents) {
            return new ArrayList<PartitionMigrationEvent>(rollbackEvents);
        }
    }

    public void clearEvents() {
        synchronized (beforeEvents) {
            beforeEvents.clear();
        }
        synchronized (commitEvents) {
            commitEvents.clear();
        }
        synchronized (rollbackEvents) {
            rollbackEvents.clear();
        }
    }

    public int size() {
        return data.size();
    }

    public Integer get(int id) {
        return data.get(id);
    }

    public boolean contains(int id) {
        return data.containsKey(id);
    }

    public Collection<Integer> keys() {
        return data.keySet();
    }
}
