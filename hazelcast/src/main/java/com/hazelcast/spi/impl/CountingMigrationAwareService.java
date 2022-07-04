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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link MigrationAwareService} that delegates to another {@link MigrationAwareService} and keeps track of the number of
 * migrations concerning the partition owner (either as current or new replica index) currently in-flight.
 */
public class CountingMigrationAwareService
        implements ChunkedMigrationAwareService, OffloadedReplicationPreparation {

    static final int PRIMARY_REPLICA_INDEX = 0;
    static final int IN_FLIGHT_MIGRATION_STAMP = -1;

    private final FragmentedMigrationAwareService migrationAwareService;
    // number of started migrations on the partition owner
    private final AtomicInteger ownerMigrationsStarted;
    // number of completed migrations on the partition owner
    private final AtomicInteger ownerMigrationsCompleted;

    public CountingMigrationAwareService(FragmentedMigrationAwareService migrationAwareService) {
        this.migrationAwareService = migrationAwareService;
        this.ownerMigrationsStarted = new AtomicInteger();
        this.ownerMigrationsCompleted = new AtomicInteger();
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (isPrimaryReplicaMigrationEvent(event)) {
            ownerMigrationsStarted.incrementAndGet();
        }
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.commitMigration(event);
        } finally {
            if (isPrimaryReplicaMigrationEvent(event)) {
                int completed = ownerMigrationsCompleted.incrementAndGet();
                assert completed <= ownerMigrationsStarted.get();
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.rollbackMigration(event);
        } finally {
            if (isPrimaryReplicaMigrationEvent(event)) {
                int completed = ownerMigrationsCompleted.incrementAndGet();
                assert completed <= ownerMigrationsStarted.get();
            }
        }
    }

    /**
     * Returns whether event involves primary replica migration.
     *
     * @param event migration event
     * @return true if migration involves primary replica, false otherwise
     */
    static boolean isPrimaryReplicaMigrationEvent(PartitionMigrationEvent event) {
        return (event.getCurrentReplicaIndex() == PRIMARY_REPLICA_INDEX || event.getNewReplicaIndex() == PRIMARY_REPLICA_INDEX);
    }

    /**
     * Returns a stamp to denote current migration state which can later be validated
     * using {@link #validateMigrationStamp(int)}.
     *
     * @return migration stamp
     */
    public int getMigrationStamp() {
        int completed = ownerMigrationsCompleted.get();
        int started = ownerMigrationsStarted.get();
        return completed == started ? completed : IN_FLIGHT_MIGRATION_STAMP;
    }

    /**
     * Returns true if there's no primary replica migrations started and/or completed
     * since issuance of the given stamp. Otherwise returns false, if there's an ongoing migration
     * when stamp is issued or a new migration is started (and optionally completed) after stamp is issued.
     *
     * @param stamp a stamp
     * @return true stamp is valid since issuance of the given stamp, false otherwise
     */
    public boolean validateMigrationStamp(int stamp) {
        int completed = ownerMigrationsCompleted.get();
        int started = ownerMigrationsStarted.get();
        return stamp == completed && stamp == started;
    }

    @Override
    public boolean shouldOffload() {
        return migrationAwareService instanceof OffloadedReplicationPreparation
                && ((OffloadedReplicationPreparation) migrationAwareService).shouldOffload();
    }

    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event, Collection<ServiceNamespace> namespace) {
        if (!(migrationAwareService instanceof ChunkedMigrationAwareService)) {
            return null;
        }
        return ((ChunkedMigrationAwareService) migrationAwareService).newChunkSupplier(event, namespace);
    }
}
