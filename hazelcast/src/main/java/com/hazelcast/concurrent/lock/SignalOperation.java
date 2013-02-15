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

import java.io.IOException;

/**
 * @mdogan 2/13/13
 */
public class SignalOperation extends BaseLockOperation {

    private boolean all;
    private String conditionId;

    public SignalOperation() {
    }

    public SignalOperation(ILockNamespace namespace, Data key, int threadId, String conditionId, boolean all) {
        super(namespace, key, threadId);
        this.conditionId = conditionId;
        this.all = all;
    }

    public void beforeRun() throws Exception {
        final LockStore lockStore = getLockStore();
        boolean isLockOwner = lockStore.isLocked(key) && lockStore.canAcquireLock(key, getCallerUuid(), threadId);
        if (!isLockOwner) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        }
    }

    public void run() throws Exception {
        LockStore lockStore = getLockStore();
        int signalCount = 1;
        final ConditionKey notifiedKey = getNotifiedKey();
        if (all) {
            signalCount = lockStore.getAwaitCount(key, conditionId);
        }
        for (int i = 0; i < signalCount; i++) {
            lockStore.registerSignalKey(notifiedKey);
        }
        response = true;
    }

    public ConditionKey getNotifiedKey() {
        return new ConditionKey(key, conditionId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(all);
        out.writeUTF(conditionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        all = in.readBoolean();
        conditionId = in.readUTF();
    }
}
