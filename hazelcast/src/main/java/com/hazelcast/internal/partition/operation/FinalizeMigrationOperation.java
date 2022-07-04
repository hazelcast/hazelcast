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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Invoked locally on the source or destination of the migration to finalize the migration.
 * This will notify the {@link MigrationAwareService}s that the migration finished, updates the replica versions,
 * clears the migration flag and notifies the node engine when successful.
 * There might be ongoing concurrent finalization operations for different partitions.
 */
public final class FinalizeMigrationOperation extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final MigrationInfo migrationInfo;
    /** Defines if this node is the source or destination of the migration. */
    private final MigrationEndpoint endpoint;
    private final boolean success;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    @SuppressWarnings("unused")
    public FinalizeMigrationOperation() {
        migrationInfo = null;
        endpoint = null;
        success = false;
    }

    public FinalizeMigrationOperation(MigrationInfo migrationInfo, MigrationEndpoint endpoint, boolean success) {
        this.migrationInfo = migrationInfo;
        this.endpoint = endpoint;
        this.success = success;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        int partitionId = migrationInfo.getPartitionId();

        if (!partitionService.getMigrationManager().removeFinalizingMigration(migrationInfo)) {
            throw new IllegalStateException("This migration is not registered as finalizing: " + migrationInfo);
        }

        if (isOldBackupReplicaOwner() && partitionStateManager.isMigrating(partitionId)) {
            // On old backup replica, migrating flag is not set during migration.
            // Because replica is copied from partition owner to new backup replica.
            // Old backup owner is not notified about migration until migration is committed
            // and completed migration is published by master.
            // If this partition's migrating flag is set, then it means another migration
            // is submitted to this member for the same partition and it's already executed.
            throw new IllegalStateException("Another replica migration is started on the same partition before finalizing "
                    + migrationInfo);
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        notifyServices();

        if (endpoint == MigrationEndpoint.SOURCE && success) {
            commitSource();
        } else if (endpoint == MigrationEndpoint.DESTINATION && !success) {
            rollbackDestination();
        }

        partitionStateManager.clearMigratingFlag(partitionId);
        if (success) {
            nodeEngine.onPartitionMigrate(migrationInfo);
        }
    }

    /**
     * Notifies all {@link MigrationAwareService}s that the migration finished. The services can then execute the commit or
     * rollback logic. If this node was the source and backup replica for a partition, the services will first be notified that
     * the migration is starting.
     */
    private void notifyServices() {
        PartitionMigrationEvent event = getPartitionMigrationEvent();

        Collection<MigrationAwareService> migrationAwareServices = getMigrationAwareServices();

        // Old backup owner is not notified about migration until migration
        // is committed on destination. This is the only place on backup owner
        // knows replica is moved away from itself.
        if (isOldBackupReplicaOwner()) {
            // execute beforeMigration on old backup before commit/rollback
            for (MigrationAwareService service : migrationAwareServices) {
                beforeMigration(event, service);
            }
        }

        for (MigrationAwareService service : migrationAwareServices) {
            finishMigration(event, service);
        }
    }

    private PartitionMigrationEvent getPartitionMigrationEvent() {
        int partitionId = getPartitionId();
        return new PartitionMigrationEvent(endpoint, partitionId,
                endpoint == MigrationEndpoint.SOURCE
                        ? migrationInfo.getSourceCurrentReplicaIndex() : migrationInfo.getDestinationCurrentReplicaIndex(),
                endpoint == MigrationEndpoint.SOURCE
                        ? migrationInfo.getSourceNewReplicaIndex() : migrationInfo.getDestinationNewReplicaIndex(),
                migrationInfo.getUid());
    }

    /** Updates the replica versions on the migration source if the replica index has changed. */
    private void commitSource() {
        int partitionId = getPartitionId();
        InternalPartitionServiceImpl partitionService = getService();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();

        ILogger logger = getLogger();

        int sourceNewReplicaIndex = migrationInfo.getSourceNewReplicaIndex();
        if (sourceNewReplicaIndex < 0) {
            clearPartitionReplicaVersions(partitionId);
            if (logger.isFinestEnabled()) {
                logger.finest("Replica versions are cleared in source after migration. partitionId=" + partitionId);
            }
        } else if (migrationInfo.getSourceCurrentReplicaIndex() != sourceNewReplicaIndex && sourceNewReplicaIndex > 1) {
            for (ServiceNamespace namespace : replicaManager.getNamespaces(partitionId)) {
                long[] versions = updatePartitionReplicaVersions(replicaManager, partitionId,
                                                                 namespace, sourceNewReplicaIndex - 1);
                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are set after SHIFT DOWN migration. partitionId="
                            + partitionId + " namespace: " + namespace + " replica versions=" + Arrays.toString(versions));
                }
            }
        }
    }

    private void clearPartitionReplicaVersions(int partitionId) {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();

        for (ServiceNamespace namespace : replicaManager.getNamespaces(partitionId)) {
            replicaManager.clearPartitionReplicaVersions(partitionId, namespace);
        }
    }

    /** Updates the replica versions on the migration destination. */
    private void rollbackDestination() {
        int partitionId = getPartitionId();
        InternalPartitionServiceImpl partitionService = getService();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        ILogger logger = getLogger();

        int destinationCurrentReplicaIndex = migrationInfo.getDestinationCurrentReplicaIndex();
        if (destinationCurrentReplicaIndex == -1) {
            clearPartitionReplicaVersions(partitionId);
            if (logger.isFinestEnabled()) {
                logger.finest("Replica versions are cleared in destination after failed migration. partitionId="
                        + partitionId);
            }
        } else {
            int replicaOffset = Math.max(migrationInfo.getDestinationCurrentReplicaIndex(), 1);

            for (ServiceNamespace namespace : replicaManager.getNamespaces(partitionId)) {
                long[] versions = updatePartitionReplicaVersions(replicaManager, partitionId, namespace, replicaOffset - 1);

                if (logger.isFinestEnabled()) {
                    logger.finest("Replica versions are rolled back in destination after failed migration. partitionId="
                            + partitionId + " namespace: " + namespace + " replica versions=" + Arrays.toString(versions));
                }
            }
        }
    }

    /** Sets all replica versions to {@code 0} up to the {@code replicaIndex}. */
    private long[] updatePartitionReplicaVersions(PartitionReplicaManager replicaManager, int partitionId,
                                                  ServiceNamespace namespace, int replicaIndex) {
        long[] versions = replicaManager.getPartitionReplicaVersions(partitionId, namespace);
        // No need to set versions back right now. actual version array is modified directly.
        Arrays.fill(versions, 0, replicaIndex, 0);

        return versions;
    }

    private void beforeMigration(PartitionMigrationEvent event, MigrationAwareService service) {
        try {
            service.beforeMigration(event);
        } catch (Throwable e) {
            getLogger().warning("Error before migration -> " + event, e);
        }
    }

    private void finishMigration(PartitionMigrationEvent event, MigrationAwareService service) {
        try {
            if (success) {
                service.commitMigration(event);
            } else {
                service.rollbackMigration(event);
            }
        } catch (Throwable e) {
            getLogger().warning("Error while finalizing migration -> " + event, e);
        }
    }

    private boolean isOldBackupReplicaOwner() {
        PartitionReplica source = migrationInfo.getSource();
        return source != null && migrationInfo.getSourceCurrentReplicaIndex() > 0
                && source.isIdentical(getNodeEngine().getLocalMember());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException();
    }
}
