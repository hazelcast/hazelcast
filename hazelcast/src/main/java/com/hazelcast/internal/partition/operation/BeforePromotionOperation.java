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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;

// Runs locally when the node becomes owner of a partition,
// before applying promotion result to the partition table.
final class BeforePromotionOperation extends AbstractPromotionOperation {

    BeforePromotionOperation(int currentReplicaIndex) {
        super(currentReplicaIndex);
    }

    @Override
    public void beforeRun() throws Exception {
        sendMigrationEvent(STARTED);
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
                logger.warning("While promoting partitionId=" + getPartitionId(), e);
            }
        }
    }
}
