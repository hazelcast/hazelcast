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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

public abstract class MultiMapBackupAwareOperation
        extends MultiMapKeyBasedOperation
        implements BackupAwareOperation, BlockingOperation {

    protected MultiMapBackupAwareOperation() {
    }

    protected MultiMapBackupAwareOperation(String name, Data dataKey, long threadId) {
        super(name, dataKey, threadId);
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        return !getOrCreateContainer().canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
