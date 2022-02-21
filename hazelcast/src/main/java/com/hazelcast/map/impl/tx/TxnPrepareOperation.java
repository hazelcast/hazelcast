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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

/**
 * An operation to prepare transaction by locking the key on the key owner.
 */
public class TxnPrepareOperation extends KeyBasedMapOperation
        implements BackupAwareOperation, MutatingOperation {

    private static final long LOCK_TTL_MILLIS = 10000L;

    private UUID ownerUuid;
    private UUID transactionId;

    protected TxnPrepareOperation(int partitionId, String name, Data dataKey,
                                  UUID ownerUuid, UUID transactionId) {
        super(name, dataKey);
        setPartitionId(partitionId);
        this.ownerUuid = ownerUuid;
        this.transactionId = transactionId;
    }

    public TxnPrepareOperation() {
    }

    @Override
    protected void runInternal() {
        try {
            wbqCapacityCounter().increment(transactionId, false);
        } catch (ReachedMaxSizeException e) {
            throw new TransactionException(e);
        }

        if (!recordStore.extendLock(getKey(), ownerUuid, getThreadId(), LOCK_TTL_MILLIS)) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Locked: [" + recordStore.isLocked(getKey()) + "], key: [" + getKey() + ']');
            }

            wbqCapacityCounter().decrement(transactionId);
            throw new TransactionException("Lock is not owned by the transaction! ["
                    + recordStore.getLockOwnerInfo(getKey()) + ']');
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
        return new TxnPrepareBackupOperation(name, dataKey, ownerUuid, getThreadId(), transactionId);
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
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", ownerUuid=")
                .append(ownerUuid)
                .append(", transactionId=")
                .append(transactionId);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_PREPARE;
    }
}
