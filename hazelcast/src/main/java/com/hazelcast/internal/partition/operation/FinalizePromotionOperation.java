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

import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionEventManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation.PromotionOperationCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;

import java.util.Arrays;

/**
 * Runs locally when the node becomes owner of a partition.
 * Finds the replica indices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
 * Therefore, it publishes {@link IPartitionLostEvent} to listeners and updates the version for the lost replicas to the
 * first available version value after the lost backups, or {@code 0} if N/A.
 * In the end it sends a {@link PartitionMigrationEvent} to notify {@link MigrationAwareService}s
 * and a {@link ReplicaMigrationEvent} to notify registered listeners of promotion commit or rollback.
 */
final class FinalizePromotionOperation extends AbstractPromotionOperation {

    private final boolean success;
    private PromotionOperationCallback finalizePromotionsCallback;
    private ILogger logger;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    FinalizePromotionOperation() {
        super(null);
        success = false;
    }

    FinalizePromotionOperation(MigrationInfo migrationInfo, boolean success, PromotionOperationCallback callback) {
        super(migrationInfo);
        this.success = success;
        this.finalizePromotionsCallback = callback;
    }

    @Override
    public void beforeRun() {
        logger = getLogger();
    }

    @Override
    public void run() {
        if (logger.isFinestEnabled()) {
            logger.finest("Running finalize promotion for " + getPartitionMigrationEvent() + ", result: " + success);
        }

        if (success) {
            migrationInfo.setStatus(MigrationStatus.SUCCESS);
            shiftUpReplicaVersions();
            commitServices();
        } else {
            rollbackServices();
        }
    }

    @Override
    public void afterRun() {
        InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        partitionStateManager.clearMigratingFlag(getPartitionId());

        if (finalizePromotionsCallback != null) {
            finalizePromotionsCallback.onComplete(migrationInfo);
        }
    }

    /**
     * Sets replica versions up to this replica to the version of the last lost replica and
     * sends a {@link IPartitionLostEvent}.
     */
    private void shiftUpReplicaVersions() {
        int partitionId = getPartitionId();
        int currentReplicaIndex = migrationInfo.getDestinationCurrentReplicaIndex();
        int lostReplicaIndex = currentReplicaIndex - 1;

        try {
            InternalPartitionServiceImpl partitionService = getService();
            PartitionReplicaVersionManager partitionReplicaVersionManager = partitionService.getPartitionReplicaVersionManager();

            for (ServiceNamespace namespace : partitionReplicaVersionManager.getNamespaces(partitionId)) {
                // returns the internal array itself, not the copy
                long[] versions = partitionReplicaVersionManager.getPartitionReplicaVersions(partitionId, namespace);

                if (currentReplicaIndex > 1) {
                    long[] versionsCopy = Arrays.copyOf(versions, versions.length);
                    long version = versions[lostReplicaIndex];
                    Arrays.fill(versions, 0, lostReplicaIndex, version);

                    if (logger.isFinestEnabled()) {
                        logger.finest(
                                "Partition replica is lost! partitionId=" + partitionId
                                        + " namespace: " + namespace + " lost replicaIndex=" + lostReplicaIndex
                                        + " replica versions before shift up=" + Arrays.toString(versionsCopy)
                                        + " replica versions after shift up=" + Arrays.toString(versions));
                    }
                } else if (logger.isFinestEnabled()) {
                    logger.finest("PROMOTE partitionId=" + getPartitionId() + " namespace: " + namespace
                            + " from currentReplicaIndex=" + currentReplicaIndex);
                }
            }

            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendPartitionLostEvent(partitionId, lostReplicaIndex);
        } catch (Throwable e) {
            logger.warning("Promotion failed. partitionId=" + partitionId + " replicaIndex=" + currentReplicaIndex, e);
        }
    }

    /** Calls commit on all {@link MigrationAwareService}. */
    private void commitServices() {
        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.commitMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting " + getPartitionMigrationEvent(), e);
            }
        }
    }

    /** Calls rollback on all {@link MigrationAwareService}. */
    private void rollbackServices() {
        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.rollbackMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting " + getPartitionMigrationEvent(), e);
            }
        }
    }
}
