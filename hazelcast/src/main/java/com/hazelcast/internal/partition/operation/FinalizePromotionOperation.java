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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionEventManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;

import java.util.Arrays;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.COMPLETED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.FAILED;

// Runs locally when the node becomes owner of a partition
//
// Finds the replica indices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
// Therefore, it publishes InternalPartitionLostEvent objects to notify related services. It also updates the version for the
// lost replicas to the first available version value after the lost backups, or 0 if N/A
final class FinalizePromotionOperation extends AbstractPromotionOperation {

    private final boolean success;
    private ILogger logger;

    FinalizePromotionOperation(int currentReplicaIndex, boolean success) {
        super(currentReplicaIndex);
        this.success = success;
    }

    private void initLogger() {
        logger = getLogger();
    }

    @Override
    public void beforeRun() throws Exception {
        initLogger();
    }

    @Override
    public void run() throws Exception {
        if (logger.isFinestEnabled()) {
            logger.finest("Running finalize promotion for " + getPartitionMigrationEvent() + ", result: " + success);
        }

        if (success) {
            shiftUpReplicaVersions();
            commitServices();
        } else {
            rollbackServices();
        }
    }

    @Override
    public void afterRun() throws Exception {
        clearPartitionMigratingFlag();
        MigrationEvent.MigrationStatus status = success ? COMPLETED : FAILED;
        sendMigrationEvent(status);
    }

    private void shiftUpReplicaVersions() {
        final int partitionId = getPartitionId();
        try {
            final InternalPartitionServiceImpl partitionService = getService();
            // returns the internal array itself, not the copy
            final long[] versions = partitionService.getPartitionReplicaVersions(partitionId);

            final int lostReplicaIndex = currentReplicaIndex - 1;

            if (currentReplicaIndex > 1) {
                final long[] versionsCopy = Arrays.copyOf(versions, versions.length);
                final long version = versions[lostReplicaIndex];
                Arrays.fill(versions, 0, lostReplicaIndex, version);

                if (logger.isFinestEnabled()) {
                    logger.finest(
                            "Partition replica is lost! partitionId=" + partitionId + " lost replicaIndex=" + lostReplicaIndex
                                    + " replica versions before shift up=" + Arrays.toString(versionsCopy)
                                    + " replica versions after shift up=" + Arrays.toString(versions));
                }
            } else if (logger.isFinestEnabled()) {
                logger.finest("PROMOTE partitionId=" + getPartitionId() + " from currentReplicaIndex=" + currentReplicaIndex);
            }

            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendPartitionLostEvent(partitionId, lostReplicaIndex);
        } catch (Throwable e) {
            logger.warning("Promotion failed. partitionId=" + partitionId + " replicaIndex=" + currentReplicaIndex, e);
        }
    }

    private void commitServices() {
        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.commitMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting partitionId=" + getPartitionId(), e);
            }
        }
    }

    private void rollbackServices() {
        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.rollbackMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting partitionId=" + getPartitionId(), e);
            }
        }
    }

    private void clearPartitionMigratingFlag() {
        final InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        final InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(getPartitionId());
        partition.setMigrating(false);
    }
}
