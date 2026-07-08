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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import java.util.UUID;
import java.util.function.Consumer;


public class OptimizeBackupOperation extends BaseOptimizeOperation implements BackupOperation, BlockingBackupOperation {

    public OptimizeBackupOperation() {
    }

    public OptimizeBackupOperation(UUID uuid, String vectorCollectionName, String indexName) {
        super(uuid, vectorCollectionName, indexName);
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.OPTIMIZE_BACKUP;
    }

    @Override
    public void setBackupOpAfterRun(Consumer<Operation> backupOpAfterRun) {
        this.backupOpAfterRun = backupOpAfterRun;
    }

    @Override
    public boolean shouldKeepAfterMigration(PartitionMigrationEvent event) {
        return event.getNewReplicaIndex() <= storage.getConfig().getTotalBackupCount();
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        // For optimize we do not request replica sync. If the optimization invocation is lost,
        // the index is still usable, it may be just a bit less efficient or larger.
        // This can will be sorted out automatically later, eg. after next optimization invocation
        // by the user or partition migration.
        // Also, when automatic periodic optimize is implemented, this will be yet another way
        // how this can heal on its own.
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest(String.format("Backup operation failed %s %s", (waited ? " (after waiting) " : " "), this), e);
        }
        super.onExecutionFailure(e);
    }
}
