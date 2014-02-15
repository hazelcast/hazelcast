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
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

abstract class BaseSignalOperation extends BaseLockOperation {

    protected boolean all;
    protected String conditionId;
    protected transient int awaitCount;

    public BaseSignalOperation() {
    }

    public BaseSignalOperation(ObjectNamespace namespace, Data key, long threadId, String conditionId, boolean all) {
        super(namespace, key, threadId);
        this.conditionId = conditionId;
        this.all = all;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        awaitCount = lockStore.getAwaitCount(key, conditionId);
        int signalCount;
        if (awaitCount > 0) {
            if (all) {
                signalCount = awaitCount;
            } else {
                signalCount = 1;
            }
        } else {
            signalCount = 0;
        }
        ConditionKey notifiedKey = new ConditionKey(namespace.getObjectName(), key, conditionId);
        for (int i = 0; i < signalCount; i++) {
            lockStore.registerSignalKey(notifiedKey);
        }
        response = true;
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
