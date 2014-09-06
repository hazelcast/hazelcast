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
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

public class BeforeAwaitOperation extends BaseLockOperation implements Notifier, BackupAwareOperation {

    private String conditionId;

    public BeforeAwaitOperation() {
    }

    public BeforeAwaitOperation(ObjectNamespace namespace, Data key, long threadId, String conditionId) {
        super(namespace, key, threadId);
        this.conditionId = conditionId;
    }

    @Override
    public void beforeRun() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        boolean isLockOwner = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        ensureOwner(lockStore, isLockOwner);
    }

    private void ensureOwner(LockStoreImpl lockStore, boolean isLockOwner) {
        if (!isLockOwner) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> "
                    + lockStore.getOwnerInfo(key));
        }
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        lockStore.addAwait(key, conditionId, getCallerUuid(), threadId);
        lockStore.unlock(key, getCallerUuid(), threadId);
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new BeforeAwaitBackupOperation(namespace, key, threadId, conditionId, getCallerUuid());
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(namespace, key);
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.BEFORE_AWAIT;
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
