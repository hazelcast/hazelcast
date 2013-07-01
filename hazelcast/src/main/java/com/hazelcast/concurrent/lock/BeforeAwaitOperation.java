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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;

import java.io.IOException;

public class BeforeAwaitOperation extends BaseLockOperation implements Notifier, BackupAwareOperation {

    private String conditionId;

    public BeforeAwaitOperation() {
    }

    public BeforeAwaitOperation(ObjectNamespace namespace, Data key, int threadId, String conditionId) {
        super(namespace, key, threadId);
        this.conditionId = conditionId;
    }

    public void beforeRun() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        boolean isLockOwner = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        if (!isLockOwner) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! " +
                    "-> Owner: " + lockStore.getLockOwnerString(key));
        }
    }

    public void run() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        lockStore.addAwait(key, conditionId, getCallerUuid(), threadId);
        lockStore.unlock(key, getCallerUuid(), threadId);
    }

    public boolean shouldNotify() {
        return true;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new BeforeAwaitBackupOperation(namespace, key, threadId, conditionId, getCallerUuid());
    }

    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(namespace, key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(conditionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        conditionId = in.readUTF();
    }
}
