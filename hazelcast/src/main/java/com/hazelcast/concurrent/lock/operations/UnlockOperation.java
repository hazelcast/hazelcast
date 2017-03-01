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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static java.lang.Boolean.TRUE;

public class UnlockOperation extends AbstractLockOperation implements Notifier, BackupAwareOperation, MutatingOperation {

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
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + ownerInfo);
        }
    }

    protected final void forceUnlock() {
        LockStoreImpl lockStore = getLockStore();
        response = lockStore.forceUnlock(key);
    }

    @Override
    public void afterRun() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        AwaitOperation awaitOperation = lockStore.pollExpiredAwaitOp(key);
        if (awaitOperation != null) {
            awaitOperation.runExpired();
        }
        shouldNotify = awaitOperation == null;
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
        return shouldNotify;
    }

    @Override
    public final WaitNotifyKey getNotifiedKey() {
        LockStoreImpl lockStore = getLockStore();
        return lockStore.getNotifiedKey(key);
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
