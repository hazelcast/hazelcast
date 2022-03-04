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
import com.hazelcast.map.impl.operation.LockAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

/**
 * An operation to unlock key on the partition owner.
 */
public class TxnUnlockOperation extends LockAwareOperation
        implements MapTxnOperation, BackupAwareOperation, MutatingOperation {

    private long version;
    private UUID ownerUuid;

    public TxnUnlockOperation() {
    }

    public TxnUnlockOperation(String name, Data dataKey, long version) {
        super(name, dataKey);
        this.version = version;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (!recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            throw new TransactionException("Cannot acquire lock UUID: "
                    + ownerUuid + ", threadId: " + threadId);
        }
    }

    @Override
    protected void runInternal() {
        recordStore.unlock(dataKey, ownerUuid, threadId, getCallId());
    }

    @Override
    public boolean shouldWait() {
        return false;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnUnlockBackupOperation(name, dataKey, ownerUuid, getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
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
    public void setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    @Override
    public void setTransactionId(UUID transactionId) {
        // NOP
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
        ownerUuid = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_UNLOCK;
    }
}
