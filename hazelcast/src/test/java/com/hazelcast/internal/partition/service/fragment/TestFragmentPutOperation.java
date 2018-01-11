/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.service.fragment;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespaceAware;

import java.io.IOException;

public class TestFragmentPutOperation extends TestAbstractFragmentOperation implements BackupAwareOperation, ServiceNamespaceAware {

    private int value;

    public TestFragmentPutOperation() {
    }

    public TestFragmentPutOperation(String name, int value) {
        super(name);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        TestFragmentedMigrationAwareService service = getService();
        service.put(name, getPartitionId(), value);
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        TestFragmentedMigrationAwareService service = getService();
        return service.backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new TestFragmentBackupPutOperation(name, value);
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
