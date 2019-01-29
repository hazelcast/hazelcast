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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.ConditionKey;
import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class AwaitOperation extends AbstractLockOperation
        implements BlockingOperation, BackupAwareOperation, MutatingOperation {

    private String conditionId;
    private boolean expired;

    public AwaitOperation() {
    }

    public AwaitOperation(ObjectNamespace namespace, Data key, long threadId, long timeout, String conditionId) {
        super(namespace, key, threadId, timeout);
        this.conditionId = conditionId;
    }

    public AwaitOperation(ObjectNamespace namespace, Data key, long threadId, long timeout, String conditionId,
                          long referenceId) {
        super(namespace, key, threadId, timeout);
        this.conditionId = conditionId;
        setReferenceCallId(referenceId);
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        if (!lockStore.lock(key, getCallerUuid(), threadId, getReferenceCallId(), leaseTime)) {
            throw new IllegalMonitorStateException(
                    "Current thread is not owner of the lock! -> " + lockStore.getOwnerInfo(key));
        }

        if (expired) {
            response = false;
        } else {
            lockStore.removeSignalKey(getWaitKey());
            lockStore.removeAwait(key, conditionId, getCallerUuid(), threadId);
            response = true;
        }
    }

    void runExpired() {
        LockStoreImpl lockStore = getLockStore();
        boolean locked = lockStore.lock(key, getCallerUuid(), threadId, getReferenceCallId(), leaseTime);
        assert locked : "Expired await operation should have acquired the lock!";
        sendResponse(false);
    }

    @Override
    public ConditionKey getWaitKey() {
        return new ConditionKey(namespace.getObjectName(), key, conditionId, getCallerUuid(), threadId);
    }

    @Override
    public boolean shouldWait() {
        LockStoreImpl lockStore = getLockStore();
        boolean canAcquireLock = lockStore.canAcquireLock(key, getCallerUuid(), threadId);
        if (!canAcquireLock) {
            return true;
        }

        return !lockStore.hasSignalKey(getWaitKey());
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new AwaitBackupOperation(namespace, key, threadId, conditionId, getCallerUuid());
    }

    @Override
    public void onWaitExpire() {
        expired = true;
        LockStoreImpl lockStore = getLockStore();
        lockStore.removeSignalKey(getWaitKey());
        lockStore.removeAwait(key, conditionId, getCallerUuid(), threadId);

        boolean locked = lockStore.lock(key, getCallerUuid(), threadId, getReferenceCallId(), leaseTime);
        if (locked) {
            // expired & acquired lock, send FALSE
            sendResponse(false);
        } else {
            // expired but could not acquire lock, no response atm
            lockStore.registerExpiredAwaitOp(this);
        }
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.AWAIT;
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
