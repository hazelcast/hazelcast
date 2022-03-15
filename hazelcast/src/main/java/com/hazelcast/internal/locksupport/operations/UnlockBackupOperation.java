/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.services.ObjectNamespace;

import java.io.IOException;
import java.util.UUID;

public class UnlockBackupOperation extends AbstractLockOperation implements BackupOperation {

    private boolean force;
    private UUID originalCallerUuid;

    public UnlockBackupOperation() {
    }

    public UnlockBackupOperation(
            ObjectNamespace namespace, Data key, long threadId, UUID originalCallerUuid, boolean force) {
        super(namespace, key, threadId);
        this.force = force;
        this.originalCallerUuid = originalCallerUuid;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        boolean unlocked;
        if (force) {
            unlocked = lockStore.forceUnlock(key);
        } else {
            unlocked = lockStore.unlock(key, originalCallerUuid, threadId, getReferenceCallId());
        }

        response = unlocked;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.UNLOCK_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, originalCallerUuid);
        out.writeBoolean(force);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        originalCallerUuid = UUIDSerializationUtil.readUUID(in);
        force = in.readBoolean();
    }
}
