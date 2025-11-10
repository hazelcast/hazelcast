/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport.operations;

import com.hazelcast.internal.locksupport.LockDataSerializerHook;
import com.hazelcast.internal.locksupport.LockStoreImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

import static java.lang.Boolean.TRUE;

public class UnlockOperation extends AbstractLockOperation implements Notifier, BackupAwareOperation, MutatingOperation {

    private boolean force;

    public UnlockOperation() {
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, long threadId) {
        super(namespace, key, threadId);
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, long threadId, boolean force) {
        super(namespace, key, threadId);
        this.force = force;
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, long threadId, boolean force, long referenceId) {
        super(namespace, key, threadId);
        this.force = force;
        this.setReferenceCallId(referenceId);
    }

    @Override
    public void run() throws Exception {
        if (force) {
            forceUnlock();
        } else {
            unlock();
        }
    }

    protected final void unlock() {
        LockStoreImpl lockStore = getLockStore();
        boolean unlocked = lockStore.unlock(key, getCallerUuid(), threadId, getReferenceCallId());
        response = unlocked;
        if (!unlocked) {
            // we can not check for retry here, hence just throw the exception
            String ownerInfo = lockStore.getOwnerInfo(key);
            throw new IllegalMonitorStateException(
                    String.format("Current thread is not owner of the lock! Key: %s, TID: %d, Name: %s, Owner info: %s",
                            key, threadId, namespace.getObjectName(), ownerInfo));
        } else {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Released lock %s", namespace.getObjectName());
            }
        }
    }

    protected final void forceUnlock() {
        LockStoreImpl lockStore = getLockStore();
        boolean unlocked = lockStore.forceUnlock(key);
        this.response = unlocked;

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            if (unlocked) {
                logger.finest("Released lock %s", namespace.getObjectName());
            } else {
                logger.finest("Could not release lock %s as it is not locked", namespace.getObjectName());
            }
        }
    }

    @Override
    public Operation getBackupOperation() {
        UnlockBackupOperation operation = new UnlockBackupOperation(namespace, key, threadId,
                getCallerUuid(), force);
        operation.setReferenceCallId(getReferenceCallId());
        return operation;
    }

    @Override
    public boolean shouldBackup() {
        return TRUE.equals(response);
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public final WaitNotifyKey getNotifiedKey() {
        LockStoreImpl lockStore = getLockStore();
        return lockStore.getNotifiedKey(key);
    }

    @Override
    public int getClassId() {
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
