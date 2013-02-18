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
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

public class AwaitOperation extends BaseLockOperation implements WaitSupport, Notifier {

    private String conditionId;
    private transient boolean isLockOwner;
    private transient boolean awaiting = false;
    private transient boolean expired = false;

    public AwaitOperation() {
    }

    public AwaitOperation(ILockNamespace namespace, Data key, int threadId, long timeout, String conditionId) {
        super(namespace, key, threadId, timeout);
        this.conditionId = conditionId;
    }

    public void beforeRun() throws Exception {
        final LockStore lockStore = getLockStore();
        awaiting = lockStore.isAwaiting(key, conditionId, getCallerUuid(), threadId);

        if (!awaiting) {
            isLockOwner = lockStore.isLocked(key) && lockStore.canAcquireLock(key, getCallerUuid(), threadId);
            if (!isLockOwner) {
                throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
            }
        }
    }

    public void run() throws Exception {
        final LockStore lockStore = getLockStore();
        if (!awaiting) {
            lockStore.addAwait(key, conditionId, getCallerUuid(), threadId);
            lockStore.unlock(key, getCallerUuid(), threadId);
            getNodeEngine().getWaitNotifyService().await(this);
        } else {
            if (!lockStore.lock(key, getCallerUuid(), threadId)) {
                throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
            }
            if (!expired) {
                lockStore.removeSignalKey(getWaitKey());
                lockStore.removeAwait(key, conditionId, getCallerUuid(), threadId);
                response = true;
            } else {
                response = false;
            }
        }
    }

    public boolean returnsResponse() {
        return awaiting || !isLockOwner;
    }

    public boolean shouldNotify() {
        return !awaiting;
    }

    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(namespace, key);
    }

    public ConditionKey getWaitKey() {
        return new ConditionKey(key, conditionId);
    }

    public boolean shouldWait() {
        return awaiting && !getLockStore().canAcquireLock(key, getCallerUuid(), threadId);
    }

    public long getWaitTimeoutMillis() {
        return timeout;
    }

    public void onWaitExpire() {
        expired = true;
        final ConditionKey waitKey = getWaitKey();
        final LockStore lockStore = getLockStore();
        lockStore.removeSignalKey(waitKey);
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
