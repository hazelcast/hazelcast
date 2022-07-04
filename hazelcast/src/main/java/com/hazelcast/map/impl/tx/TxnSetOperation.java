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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.BasePutOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * An operation to unlock and set (key,value) on the partition .
 */
public class TxnSetOperation extends BasePutOperation
        implements MapTxnOperation, MutatingOperation {

    private long ttl;
    private long version;
    private UUID ownerUuid;
    private UUID transactionId;

    private transient boolean shouldBackup;

    public TxnSetOperation() {
    }

    public TxnSetOperation(String name, Data dataKey,
                           Data value, long version, long ttl) {
        super(name, dataKey, value);
        this.version = version;
        this.ttl = ttl;
    }

    @Override
    public boolean shouldWait() {
        return false;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (!recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            wbqCapacityCounter().decrement(transactionId);
            throw new TransactionException("Cannot acquire lock UUID: " + ownerUuid + ", threadId: " + threadId);
        }
    }

    @Override
    protected void runInternal() {
        recordStore.unlock(dataKey, ownerUuid, threadId, getCallId());
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null || version == record.getVersion()) {
            EventService eventService = getNodeEngine().getEventService();
            if (eventService.hasEventRegistration(MapService.SERVICE_NAME, getName())) {
                oldValue = record == null ? null : mapServiceContext.toData(record.getValue());
            }
            eventType = record == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
            recordStore.setTxn(dataKey, dataValue, ttl, UNSET, transactionId);
            shouldBackup = true;
        }
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
    public void setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    @Override
    public void setTransactionId(UUID transactionId) {
        this.transactionId = transactionId;
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
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup && super.shouldBackup();
    }

    @Override
    public Operation getBackupOperation() {
        Record record = recordStore.getRecord(dataKey);
        dataValue = getValueOrPostProcessedValue(record, dataValue);
        ExpiryMetadata expiryMetadata = recordStore.getExpirySystem().getExpiryMetadata(dataKey);
        return new TxnSetBackupOperation(name, dataKey,
                record, dataValue, expiryMetadata, transactionId);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
        out.writeLong(ttl);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
        ttl = in.readLong();
        ownerUuid = UUIDSerializationUtil.readUUID(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_SET;
    }
}
