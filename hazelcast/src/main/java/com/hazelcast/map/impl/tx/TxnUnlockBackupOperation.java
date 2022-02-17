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

package com.hazelcast.map.impl.tx;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.UUID;

/**
 * An operation to unlock key on the backup owner.
 */
public class TxnUnlockBackupOperation
        extends KeyBasedMapOperation implements BackupOperation {

    private UUID ownerUuid;

    public TxnUnlockBackupOperation() {
    }

    public TxnUnlockBackupOperation(String name, Data dataKey,
                                    UUID ownerUuid, long lockThreadId) {
        super(name, dataKey);
        this.ownerUuid = ownerUuid;
        this.threadId = lockThreadId;
    }

    @Override
    protected void runInternal() {
        recordStore.unlock(dataKey, ownerUuid, threadId, getCallId());
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_UNLOCK_BACKUP;
    }
}
