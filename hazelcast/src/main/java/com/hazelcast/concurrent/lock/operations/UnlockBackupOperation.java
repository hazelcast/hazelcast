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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

public class UnlockBackupOperation extends BaseLockOperation implements BackupOperation {

    private boolean force;
    private String originalCallerUuid;

    public UnlockBackupOperation() {
    }

    public UnlockBackupOperation(
            ObjectNamespace namespace, Data key, long threadId, String originalCallerUuid, boolean force) {
        super(namespace, key, threadId);
        this.force = force;
        this.originalCallerUuid = originalCallerUuid;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        if (force) {
            response = lockStore.forceUnlock(key);
        } else {
            response = lockStore.unlock(key, originalCallerUuid, threadId);
        }
        lockStore.pollExpiredAwaitOp(key);
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.UNLOCK_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(originalCallerUuid);
        out.writeBoolean(force);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        originalCallerUuid = in.readUTF();
        force = in.readBoolean();
    }
}
