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

import com.hazelcast.concurrent.lock.ConditionKey;
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
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

public class UnlockOperation extends BaseLockOperation implements Notifier, BackupAwareOperation {

    private boolean force;
    private boolean shouldNotify;

    public UnlockOperation() {
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, long threadId) {
        super(namespace, key, threadId);
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, long threadId, boolean force) {
        super(namespace, key, threadId);
        this.force = force;
    }

    @Override
    public void run() throws Exception {
        if (force) {
            forceUnlock();
        } else {
            unlock();
        }
    }

    private void unlock() {
        LockStoreImpl lockStore = getLockStore();
        boolean unlocked = lockStore.unlock(key, getCallerUuid(), threadId);
        response = unlocked;
        ensureUnlocked(lockStore, unlocked);
    }

    private void ensureUnlocked(LockStoreImpl lockStore, boolean unlocked) {
        if (!unlocked) {
            String ownerInfo = lockStore.getOwnerInfo(key);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + ownerInfo);
        }
    }

    private void forceUnlock() {
        LockStoreImpl lockStore = getLockStore();
        response = lockStore.forceUnlock(key);
    }

    @Override
    public void afterRun() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        AwaitOperation awaitResponse = lockStore.pollExpiredAwaitOp(key);
        if (awaitResponse != null) {
            OperationService operationService = getNodeEngine().getOperationService();
            operationService.runOperationOnCallingThread(awaitResponse);
        }
        shouldNotify = awaitResponse == null;
    }

    @Override
    public Operation getBackupOperation() {
        return new UnlockBackupOperation(namespace, key, threadId, getCallerUuid(), force);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public boolean shouldNotify() {
        return shouldNotify;
    }

    @Override
    public final WaitNotifyKey getNotifiedKey() {
        LockStoreImpl lockStore = getLockStore();
        ConditionKey conditionKey = lockStore.getSignalKey(key);
        if (conditionKey == null) {
            return new LockWaitNotifyKey(namespace, key);
        } else {
            return conditionKey;
        }
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(force);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        force = in.readBoolean();
    }
}
