/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.operations;

import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

/**
 * @author ali 1/16/13
 */
public abstract class MultiMapBackupAwareOperation extends MultiMapKeyBasedOperation
        implements BackupAwareOperation, WaitSupport {

    protected long threadId;

    protected MultiMapBackupAwareOperation() {
    }

    protected MultiMapBackupAwareOperation(String name, Data dataKey, long threadId) {
        super(name, dataKey);
        this.threadId = threadId;
    }

    public boolean shouldBackup() {
        return response != null;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(threadId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        threadId = in.readLong();
    }

    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    public boolean shouldWait() {
        return !getOrCreateContainer().canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
