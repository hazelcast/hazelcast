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

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.internal.locksupport.LockDataSerializerHook;
import com.hazelcast.internal.locksupport.LockStoreImpl;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.UUID;

public class LockBackupOperation extends AbstractLockOperation implements BackupOperation {

    private UUID originalCallerUuid;

    public LockBackupOperation() {
    }

    public LockBackupOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, UUID originalCallerUuid
            , boolean isClient) {
        super(namespace, key, threadId);
        this.leaseTime = leaseTime;
        this.originalCallerUuid = originalCallerUuid;
        this.isClient = isClient;
    }

    @Override
    public void run() throws Exception {
        if (isClient) {
            ClientEngine clientEngine = getNodeEngine().getService(ClientEngineImpl.SERVICE_NAME);
            clientEngine.onClientAcquiredResource(originalCallerUuid);
        }
        interceptLockOperation();
        LockStoreImpl lockStore = getLockStore();
        response = lockStore.lock(key, originalCallerUuid, threadId, getReferenceCallId(), leaseTime);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.LOCK_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, originalCallerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        originalCallerUuid = UUIDSerializationUtil.readUUID(in);
    }
}
