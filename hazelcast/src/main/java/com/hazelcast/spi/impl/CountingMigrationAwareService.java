/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final MigrationStamps migrationStamps = new MigrationStamps();

    public CountingMigrationAwareService(FragmentedMigrationAwareService migrationAwareService) {
        this.migrationAwareService = migrationAwareService;
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
        migrationStamps.start(event);
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.commitMigration(event);
        } finally {
            migrationStamps.complete(event);
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.rollbackMigration(event);
        } finally {
            migrationStamps.complete(event);
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
        return migrationStamps.getMigrationStamp();
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
        return migrationStamps.validateMigrationStamp(stamp);
    }

    /**
     * Returns a stamp for the migration state of the given partition which can later be validated
     * using {@link #validatePartitionMigrationStamp(int, int)}.
     *
     * @param partitionId partition ID
     * @return partition migration stamp
     */
    public int getPartitionMigrationStamp(int partitionId) {
        return migrationStamps.getPartitionMigrationStamp(partitionId);
    }

    /**
     * Returns {@code true} if no primary replica migration for the given partition has started
     * since the given stamp was issued.
     *
     * @param partitionId partition ID
     * @param stamp partition migration stamp
     * @return {@code true} if the stamp is still valid, {@code false} otherwise
     */
    public boolean validatePartitionMigrationStamp(int partitionId, int stamp) {
        return migrationStamps.validatePartitionMigrationStamp(partitionId, stamp);
    }

    @Override
    public boolean shouldOffload() {
        return migrationAwareService instanceof OffloadedReplicationPreparation orp
                && orp.shouldOffload();
    }

    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event, Collection<ServiceNamespace> namespace) {
        if (!(migrationAwareService instanceof ChunkedMigrationAwareService)) {
            return null;
        }
        return ((ChunkedMigrationAwareService) migrationAwareService).newChunkSupplier(event, namespace);
    }

    private static final class MigrationStamps {
        private final MigrationCounters nodeWide = new MigrationCounters();
        private final ConcurrentMap<Integer, MigrationCounters> byPartition = new ConcurrentHashMap<>();

        private void start(PartitionMigrationEvent event) {
            if (isPrimaryReplicaMigrationEvent(event)) {
                nodeWide.start();
                getPartition(event.getPartitionId()).start();
            }
        }

        private void complete(PartitionMigrationEvent event) {
            if (isPrimaryReplicaMigrationEvent(event)) {
                nodeWide.complete();
                getPartition(event.getPartitionId()).complete();
            }
        }

        private int getMigrationStamp() {
            return nodeWide.getStamp();
        }

        private boolean validateMigrationStamp(int stamp) {
            return nodeWide.validateStamp(stamp);
        }

        private int getPartitionMigrationStamp(int partitionId) {
            return getPartition(partitionId).getStamp();
        }

        private boolean validatePartitionMigrationStamp(int partitionId, int stamp) {
            return getPartition(partitionId).validateStamp(stamp);
        }

        private MigrationCounters getPartition(int partitionId) {
            return byPartition.computeIfAbsent(partitionId, ignored -> new MigrationCounters());
        }
    }

    private static final class MigrationCounters {
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();

        private void start() {
            started.incrementAndGet();
        }

        private void complete() {
            int completedCount = completed.incrementAndGet();
            assert completedCount <= started.get();
        }

        private int getStamp() {
            int completedCount = completed.get();
            int startedCount = started.get();
            return completedCount == startedCount ? completedCount : IN_FLIGHT_MIGRATION_STAMP;
        }

        private boolean validateStamp(int stamp) {
            return stamp == completed.get() && stamp == started.get();
        }
    }
}
