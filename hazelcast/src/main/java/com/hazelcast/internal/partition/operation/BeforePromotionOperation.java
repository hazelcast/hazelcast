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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;

/**
 * Runs locally when the node becomes owner of a partition, before applying a promotion result to the partition table.
 * Sends a {@link MigrationEvent} and notifies all {@link MigrationAwareService}s that the migration is starting.
 * After completion notifies the {@link #beforePromotionsCallback}.
 */
final class BeforePromotionOperation extends AbstractPromotionOperation {

    private Runnable beforePromotionsCallback;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    public BeforePromotionOperation() {
        super(null);
    }

    BeforePromotionOperation(MigrationInfo migrationInfo, Runnable beforePromotionsCallback) {
        super(migrationInfo);
        this.beforePromotionsCallback = beforePromotionsCallback;
    }

    @Override
    public void beforeRun() throws Exception {
        sendMigrationEvent(STARTED);

        InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        partitionStateManager.setMigratingFlag(getPartitionId());
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Running before promotion for " + getPartitionMigrationEvent());
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
    public void afterRun() throws Exception {
        if (beforePromotionsCallback != null) {
            beforePromotionsCallback.run();
        }
    }
}
