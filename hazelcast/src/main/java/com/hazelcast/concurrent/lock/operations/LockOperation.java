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
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.spi.CallStatus.WAIT;

public class LockOperation extends AbstractLockOperation implements BlockingOperation, BackupAwareOperation, MutatingOperation {

    private transient boolean response;

    public LockOperation() {
    }

    public LockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout) {
        super(namespace, key, threadId, leaseTime, timeout);
    }

    public LockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout, long referenceId) {
        super(namespace, key, threadId, leaseTime, timeout);
        setReferenceCallId(referenceId);
    }

    @Override
    public Object call() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        if (shouldWait(lockStore)) {
            //System.out.println("Lock.lock IsBlocked");
            return WAIT;
        }

        //System.out.println("Lock.lock Is not blocked");

        response = lockStore.lock(key, getCallerUuid(), threadId, getReferenceCallId(), leaseTime);
        return response;
    }

    public final boolean shouldWait(LockStoreImpl lockStore) {
        return getWaitTimeout() != 0 && !lockStore.canAcquireLock(key, getCallerUuid(), threadId);
    }

    @Override
    public Operation getBackupOperation() {
        LockBackupOperation operation = new LockBackupOperation(namespace, key, threadId, leaseTime, getCallerUuid());
        operation.setReferenceCallId(getReferenceCallId());
        return operation;
    }

    @Override
    public boolean shouldBackup() {
        return response;
    }

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(namespace, key);
    }

    @Override
    public final void onWaitExpire() {
        Object response;
        long timeout = getWaitTimeout();
        if (timeout < 0 || timeout == Long.MAX_VALUE) {
            response = new OperationTimeoutException();
        } else {
            response = Boolean.FALSE;
        }
        sendResponse(response);
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.LOCK;
    }
}
