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
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

public class AwaitOperation extends BaseLockOperation implements WaitSupport, BackupAwareOperation {

    private String conditionId;
    private transient boolean firstRun = false;
    private transient boolean expired = false;

    public AwaitOperation() {
    }

    public AwaitOperation(ObjectNamespace namespace, Data key, int threadId, long timeout, String conditionId) {
        super(namespace, key, threadId, timeout);
        this.conditionId = conditionId;
    }

    public void beforeRun() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        firstRun = lockStore.startAwaiting(key, conditionId, getCallerUuid(), threadId);
    }

    public void run() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        if (!lockStore.lock(key, getCallerUuid(), threadId)) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + lockStore.getOwnerInfo(key));
        }
        if (!expired) {
            lockStore.removeSignalKey(getWaitKey());
            lockStore.removeAwait(key, conditionId, getCallerUuid(), threadId);
            response = true;
        } else {
            response = false;
        }
    }

    public ConditionKey getWaitKey() {
        return new ConditionKey(key, conditionId);
    }

    public boolean shouldWait() {
        final boolean shouldWait = firstRun || !getLockStore().canAcquireLock(key, getCallerUuid(), threadId);
        firstRun = false;
        return shouldWait;
    }

    public long getWaitTimeoutMillis() {
        return timeout;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new AwaitBackupOperation(namespace, key, threadId, conditionId, getCallerUuid());
    }

    public void onWaitExpire() {
        expired = true;
        final LockStoreImpl lockStore = getLockStore();
        lockStore.removeSignalKey(getWaitKey());
        lockStore.removeAwait(key, conditionId, getCallerUuid(), threadId);

        if (lockStore.lock(key, getCallerUuid(), threadId)) {
            getResponseHandler().sendResponse(false); // expired & acquired lock, send FALSE
        } else {
            // expired but could not acquire lock, no response atm
            lockStore.registerExpiredAwaitOp(this);
        }
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
