/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;

/**
 * A periodic task to publish partition state to cluster members in a predefined interval.
 */
class PublishPartitionRuntimeStateTask implements Runnable {
    private final Node node;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    PublishPartitionRuntimeStateTask(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.partitionService = partitionService;
        logger = node.getLogger(InternalPartitionService.class);
    }

    @Override
    public void run() {
        if (node.isMaster() && node.getState() == NodeState.ACTIVE) {
            MigrationManager migrationManager = partitionService.getMigrationManager();
            final boolean migrationAllowed = migrationManager.isMigrationAllowed()
                    && !partitionService.isFetchMostRecentPartitionTableTaskRequired();
            if (!migrationAllowed) {
                logger.fine("Not publishing partition runtime state since migration is not allowed.");
                return;
            } else if (migrationManager.hasOnGoingMigration()) {
                logger.info("Remaining migration tasks in queue => " + partitionService.getMigrationQueueSize());
            }
            partitionService.publishPartitionRuntimeState();
        }
    }
}
