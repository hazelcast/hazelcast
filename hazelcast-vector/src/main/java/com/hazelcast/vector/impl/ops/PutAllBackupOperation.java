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

import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import java.io.IOException;
import java.util.function.Consumer;

public class PutAllBackupOperation extends BaseMutatingOperation
        implements PartitionAwareOperation, BackupOperation, BlockingBackupOperation {

    protected transient Consumer<Operation> backupOpAfterRun;
    private VectorEntries vectorEntries;

    public PutAllBackupOperation() {
    }

    public PutAllBackupOperation(String name, VectorEntries vectorEntries) {
        super(name);
        this.vectorEntries = vectorEntries;
    }

    @Override
    public void run() throws Exception {
        // backup operation cannot be easily offloaded and avoid problems with
        // getting different result on backup if there are interleaving operations
        // (it would have to lock the index) and with concurrent promotions.
        putAll();

        if (backupOpAfterRun != null) {
            // we were waiting - send backup acks
            backupOpAfterRun.accept(this);
        }
    }

    protected void putAll() {
        int currentIndex = 0;
        while (currentIndex < vectorEntries.size()) {
            DataVectorDocument value = vectorEntries.getDocument(currentIndex);
            storage.set(vectorEntries.getKey(currentIndex), value.getValue(), value.getVectors());
            currentIndex++;
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(vectorEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        vectorEntries = in.readObject();
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.PUT_ALL_BACKUP;
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
