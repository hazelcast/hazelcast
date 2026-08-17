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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import java.util.function.Consumer;

public class DeleteBackupOperation extends BaseDeleteOperation implements BackupOperation, BlockingBackupOperation {

    private transient Consumer<Operation> backupOpAfterRun;

    public DeleteBackupOperation() {
    }

    public DeleteBackupOperation(String vectorCollectionName, Data key) {
        super(vectorCollectionName, key);
    }

    @Override
    public void run() throws Exception {
        if (storage != null) {
            storage.delete(key);
        }

        if (backupOpAfterRun != null) {
            // we were waiting - send backup acks
            backupOpAfterRun.accept(this);
        }
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.DELETE_BACKUP;
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
