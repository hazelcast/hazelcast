/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.lock.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockStoreImpl;
import com.hazelcast.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

public class SignalOperation extends BaseSignalOperation implements BackupAwareOperation, MutatingOperation {

    public SignalOperation() {
    }

    public SignalOperation(ObjectNamespace namespace, Data key, long threadId, String conditionId, boolean all) {
        super(namespace, key, threadId, conditionId, all);
    }

    @Override
    public void beforeRun() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        boolean isLockOwner = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        ensureLockOwner(lockStore, isLockOwner);
    }

    private void ensureLockOwner(LockStoreImpl lockStore, boolean isLockOwner) {
        if (!isLockOwner) {
            String ownerInfo = lockStore.getOwnerInfo(key);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + ownerInfo);
        }
    }

    @Override
    public boolean shouldBackup() {
        return awaitCount > 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new SignalBackupOperation(namespace, key, threadId, conditionId, all);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.SIGNAL;
    }
}
