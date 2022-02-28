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
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

/**
 * An operation to prepare transaction by locking the key on key backup owner.
 */
public class TxnPrepareBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    private static final long LOCK_TTL_MILLIS = 10000L;

    private UUID lockOwnerUuid;
    private UUID transactionId;

    protected TxnPrepareBackupOperation(String name, Data dataKey, UUID lockOwnerUuid,
                                        long lockThreadId, UUID transactionId) {
        super(name, dataKey);
        this.lockOwnerUuid = lockOwnerUuid;
        this.threadId = lockThreadId;
        this.transactionId = transactionId;
    }

    public TxnPrepareBackupOperation() {
    }

    @Override
    protected void runInternal() {
        wbqCapacityCounter().increment(transactionId, true);

        if (!recordStore.txnLock(getKey(), lockOwnerUuid, threadId, getCallId(), LOCK_TTL_MILLIS, true)) {
            wbqCapacityCounter().decrement(transactionId);
            throw new TransactionException("Lock is not owned by the transaction! Caller: " + lockOwnerUuid
                    + ", Owner: " + recordStore.getLockOwnerInfo(getKey()));
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, lockOwnerUuid);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lockOwnerUuid = UUIDSerializationUtil.readUUID(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_PREPARE_BACKUP;
    }
}
