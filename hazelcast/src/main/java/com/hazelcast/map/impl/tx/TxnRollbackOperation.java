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

import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

/**
 * An operation to rollback transaction by unlocking the key on key owner.
 */
public class TxnRollbackOperation
        extends KeyBasedMapOperation implements BackupAwareOperation, Notifier {

    private UUID ownerUuid;
    private UUID transactionId;

    protected TxnRollbackOperation(int partitionId, String name, Data dataKey,
                                   UUID ownerUuid, UUID transactionId) {
        super(name, dataKey);
        setPartitionId(partitionId);
        this.ownerUuid = ownerUuid;
        this.transactionId = transactionId;
    }

    public TxnRollbackOperation() {
    }

    @Override
    protected void runInternal() {
        wbqCapacityCounter().decrement(transactionId);
        if (recordStore.isLocked(getKey())
                && !recordStore.unlock(getKey(), ownerUuid, getThreadId(), getCallId())) {
            throw new TransactionException("Lock is not owned by the transaction! Owner: "
                    + recordStore.getLockOwnerInfo(getKey()));
        }
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof TransactionException) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("failed to execute:" + this, e);
            }
            return;
        }
        super.logError(e);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public final Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, dataKey, ownerUuid, getThreadId(), transactionId);
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = UUIDSerializationUtil.readUUID(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_ROLLBACK;
    }
}
