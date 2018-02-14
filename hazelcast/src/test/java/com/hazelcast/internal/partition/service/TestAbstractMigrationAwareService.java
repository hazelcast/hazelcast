/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.ServiceNamespace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 */
public abstract class TestAbstractMigrationAwareService<N> implements ManagedService, MigrationAwareService {

    protected static final String BACKUP_COUNT_PROP = "backups.count";

    private final List<PartitionMigrationEvent> beforeEvents = new ArrayList<PartitionMigrationEvent>();

    private final List<PartitionMigrationEvent> commitEvents = new ArrayList<PartitionMigrationEvent>();

    private final List<PartitionMigrationEvent> rollbackEvents = new ArrayList<PartitionMigrationEvent>();

    public volatile int backupCount;

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

    @Override
    public final void beforeMigration(PartitionMigrationEvent event) {
        synchronized (beforeEvents) {
            beforeEvents.add(event);
        }
        onBeforeMigration(event);
    }

    protected void onBeforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public final void commitMigration(PartitionMigrationEvent event) {
        synchronized (commitEvents) {
            commitEvents.add(event);
        }
        onCommitMigration(event);
    }

    protected void onCommitMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        synchronized (rollbackEvents) {
            rollbackEvents.add(event);
        }
        onRollbackMigration(event);
    }

    protected void onRollbackMigration(PartitionMigrationEvent event) {
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

    public abstract int size(N name);

    public abstract Integer get(N name, int id);

    public abstract Collection<Integer> keys(N name);

    public abstract boolean contains(N name, int partitionId);

    public abstract String getServiceName();

    public abstract ServiceNamespace getNamespace(N name);
}
