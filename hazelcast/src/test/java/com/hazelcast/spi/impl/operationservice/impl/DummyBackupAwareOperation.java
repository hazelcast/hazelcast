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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DummyBackupAwareOperation extends Operation implements BackupAwareOperation {

    public static final ConcurrentMap<String, Integer> backupCompletedMap = new ConcurrentHashMap<String, Integer>();

    public int syncBackupCount;
    public int asyncBackupCount;
    public boolean returnsResponse = true;
    public String backupKey;

    public DummyBackupAwareOperation() {
    }

    public DummyBackupAwareOperation(int partitionId) {
        setPartitionId(partitionId);
    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    public boolean shouldBackup() {
        return true;
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
        System.out.println("DummyBackupOperation created");
        return new DummyBackupOperation(backupKey);
    }

    @Override
    public void run() throws Exception {
        System.out.println("DummyBackupAwareOperation completed");
        if (backupKey != null) {
            backupCompletedMap.put(backupKey, 0);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
        out.writeBoolean(returnsResponse);
        out.writeString(backupKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
        returnsResponse = in.readBoolean();
        backupKey = in.readString();
    }
}
