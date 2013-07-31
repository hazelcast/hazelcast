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

public class UnlockOperation extends BaseLockOperation implements Notifier, BackupAwareOperation {

    private boolean force = false;
    private transient boolean shouldNotify;

    public UnlockOperation() {
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, int threadId) {
        super(namespace, key, threadId);
    }

    public UnlockOperation(ObjectNamespace namespace, Data key, int threadId, boolean force) {
        super(namespace, key, threadId);
        this.force = force;
    }

    public void run() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        if (force) {
            response = lockStore.forceUnlock(key);
        } else {
            final boolean unlocked = lockStore.unlock(key, getCallerUuid(), threadId);
            response = unlocked;
            if (!unlocked) {
                throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + lockStore.getOwnerInfo(key));
            }
        }
    }

    public void afterRun() throws Exception {
        final AwaitOperation awaitResponse = getLockStore().pollExpiredAwaitOp(key);
        if (awaitResponse != null) {
            getNodeEngine().getOperationService().runOperation(awaitResponse);
        }
        shouldNotify = awaitResponse == null;
    }

    public Operation getBackupOperation() {
        return new UnlockBackupOperation(namespace, key, threadId, getCallerUuid(), force);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public boolean shouldNotify() {
        return shouldNotify;
    }

    public final WaitNotifyKey getNotifiedKey() {
        final ConditionKey conditionKey = getLockStore().getSignalKey(key);
        return conditionKey != null ? conditionKey : new LockWaitNotifyKey(namespace, key);
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
