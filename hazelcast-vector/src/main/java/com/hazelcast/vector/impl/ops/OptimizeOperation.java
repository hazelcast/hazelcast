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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import java.util.UUID;

public class OptimizeOperation extends BaseOptimizeOperation implements BackupAwareOperation, MutatingOperation {

    public OptimizeOperation() {
    }

    public OptimizeOperation(UUID uuid, String vectorCollectionName, String indexName) {
        super(uuid, vectorCollectionName, indexName);
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.OPTIMIZE;
    }

    @Override
    public boolean returnsResponse() {
        return storage == null;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return storage != null && storage.getConfig().getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        // optimize executes all backups as async backups. See also MultipleEntryOperation.
        // PartitionIteratingOperation does not wait for backup acks for each individual partition.
        //
        // For optimize this should be acceptable, as generally no data will be lost.
        // In case of lost backups anti-entropy will kick in or optimization will probably be
        // ultimately executed during partition migration.
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return storage != null ? storage.getConfig().getTotalBackupCount() : 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new OptimizeBackupOperation(uuid, getName(), indexName);
    }
}
