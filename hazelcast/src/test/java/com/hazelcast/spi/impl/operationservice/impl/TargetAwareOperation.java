/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.TargetAware;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TargetAwareOperation extends Operation implements TargetAware, BackupAwareOperation, PartitionAwareOperation {

    public static final List<Address> TARGETS = new CopyOnWriteArrayList<Address>();

    private int syncBackupCount;
    private int asyncBackupCount;
    private TargetAwareOperation backupOp;
    private Address targetAddress;

    TargetAwareOperation() {
        this(0, 0, -1);
    }

    TargetAwareOperation(int syncBackupCount, int asyncBackupCount, int partitionId) {
        this.syncBackupCount = syncBackupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        assert targetAddress != null;
    }

    @Override
    public Object getResponse() {
        return targetAddress;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(targetAddress);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        targetAddress = in.readObject();
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }

    @Override
    public void setTarget(Address address) {
        this.targetAddress = address;
        TARGETS.add(targetAddress);
    }

    @Override
    public boolean shouldBackup() {
        return (syncBackupCount + asyncBackupCount) > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    @Override
    public Operation getBackupOperation() {
        if (backupOp == null) {
            backupOp = new TargetAwareOperation();
        }
        return backupOp;
    }

    public Address getTargetAddress() {
        return targetAddress;
    }
}
