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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation.PromotionOperationCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;

/**
 * Runs locally when the node becomes owner of a partition, before applying a promotion result to the partition table.
 * Sends a {@link ReplicaMigrationEvent} and notifies all {@link MigrationAwareService}s that the migration is starting.
 * After completion notifies the {@link #beforePromotionsCallback}.
 */
final class BeforePromotionOperation extends AbstractPromotionOperation {

    private PromotionOperationCallback beforePromotionsCallback;
    private boolean migratingFlagSet;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    BeforePromotionOperation() {
        super(null);
    }

    BeforePromotionOperation(MigrationInfo migrationInfo, PromotionOperationCallback callback) {
        super(migrationInfo);
        this.beforePromotionsCallback = callback;
    }

    @Override
    public void beforeRun() {
        InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        migratingFlagSet = partitionStateManager.trySetMigratingFlag(getPartitionId());
        if (!migratingFlagSet) {
            // PromotionCommitOperation rolls back the promotion and the master retries it
            getLogger().fine("Cannot set migrating flag of partition %s, "
                    + "probably previous migration's finalization is not completed yet.", getPartitionId());
        }
    }

    @Override
    public void run() {
        if (!migratingFlagSet) {
            return;
        }

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Running before promotion for %s", getPartitionMigrationEvent());
        }

        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.beforeMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting " + getPartitionMigrationEvent(), e);
            }
        }
    }

    @Override
    public void afterRun() {
        if (beforePromotionsCallback == null) {
            return;
        }
        if (migratingFlagSet) {
            beforePromotionsCallback.onComplete(migrationInfo);
        } else {
            beforePromotionsCallback.onFailure(migrationInfo);
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        // afterRun() does not run after a failure. Without this, PromotionCommitOperation waits for this operation forever
        // and keeps the promotion permit. run() fails only before it calls the services, so no service state changed.
        if (migratingFlagSet) {
            InternalPartitionServiceImpl service = getService();
            service.getPartitionStateManager().clearMigratingFlag(getPartitionId());
        }
        if (beforePromotionsCallback != null) {
            beforePromotionsCallback.onFailure(migrationInfo);
        }
    }
}
