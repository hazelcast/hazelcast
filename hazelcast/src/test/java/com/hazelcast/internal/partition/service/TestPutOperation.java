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

package com.hazelcast.internal.partition.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class TestPutOperation extends Operation implements BackupAwareOperation {

    private int value;

    public TestPutOperation() {
    }

    public TestPutOperation(int value) {
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        TestMigrationAwareService service = getService();
        service.put(getPartitionId(), value);
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        TestMigrationAwareService service = getService();
        return service.backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new TestBackupPutOperation(value);
    }

    @Override
    public String getServiceName() {
        return TestMigrationAwareService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.value = in.readInt();
    }
}
