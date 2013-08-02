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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

/**
 * @author mdogan 2/13/13
 */
public class SignalOperation extends BaseSignalOperation implements BackupAwareOperation {

    public SignalOperation() {
    }

    public SignalOperation(ObjectNamespace namespace, Data key, int threadId, String conditionId, boolean all) {
        super(namespace, key, threadId, conditionId, all);
    }

    public void beforeRun() throws Exception {
        final LockStoreImpl lockStore = getLockStore();
        boolean isLockOwner = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        if (!isLockOwner) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + lockStore.getOwnerInfo(key));
        }
    }

    public boolean shouldBackup() {
        return awaitCount > 0;
    }

    public Operation getBackupOperation() {
        return new SignalBackupOperation(namespace, key, threadId, conditionId, all);
    }
}
