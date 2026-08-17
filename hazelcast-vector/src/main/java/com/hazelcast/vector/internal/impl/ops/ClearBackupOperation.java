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
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import java.util.function.Consumer;

public class ClearBackupOperation extends BaseMutatingOperation
        implements PartitionAwareOperation, BackupOperation, BlockingBackupOperation {

    private transient Consumer<Operation> backupOpAfterRun;

    public ClearBackupOperation(String vectorCollectionName) {
        super(vectorCollectionName);
    }

    public ClearBackupOperation() {
    }

    @Override
    public void run() throws Exception {
        if (storage != null) {
            storage.clear();
        }

        if (backupOpAfterRun != null) {
            // clear is PartitionIterationOperation so should not use ACKs
            // (see https://hazelcast.atlassian.net/browse/HZOLD-4859),
            // but invoke it anyway for consistency
            backupOpAfterRun.accept(this);
        }
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.CLEAR_BACKUP;
    }

    @Override
    public void setBackupOpAfterRun(Consumer<Operation> backupOpAfterRun) {
        this.backupOpAfterRun = backupOpAfterRun;
    }

    @Override
    public boolean shouldKeepAfterMigration(PartitionMigrationEvent event) {
        return event.getNewReplicaIndex() <= storage.getConfig().getTotalBackupCount();
    }
}
