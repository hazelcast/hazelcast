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

package com.hazelcast.map.tx;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.operation.BasePutOperation;
import com.hazelcast.map.operation.PutBackupOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordInfo;
import com.hazelcast.map.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * An operation to unlock and set (key,value) on the partition .
 */
public class TxnSetOperation extends BasePutOperation implements MapTxnOperation {

    private long version;
    private transient boolean shouldBackup;
    private String ownerUuid;

    public TxnSetOperation() {
    }

    public TxnSetOperation(String name, Data dataKey, Data value, long version) {
        super(name, dataKey, value);
        this.version = version;
    }

    public TxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        super(name, dataKey, value);
        this.version = version;
        this.ttl = ttl;
    }

    @Override
    public boolean shouldWait() {
        return false;
    }


    @Override
    public void innerBeforeRun() {
        if (!recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            throw new TransactionException("Cannot acquire lock uuid: " + ownerUuid + ", threadId: " + threadId);
        }
    }

    @Override
    public void run() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final EventService eventService = getNodeEngine().getEventService();
        recordStore.unlock(dataKey, ownerUuid, threadId);
        Record record = recordStore.getRecord(dataKey);
        if (record == null || version == record.getVersion()) {
            if (eventService.hasEventRegistration(MapService.SERVICE_NAME, getName())) {
                dataOldValue = record == null ? null : mapServiceContext.toData(record.getValue());
            }
            eventType = record == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
            recordStore.set(dataKey, dataValue, ttl);
            shouldBackup = true;
        }
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public void setOwnerUuid(String ownerUuid) {
        this.ownerUuid = ownerUuid;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    public boolean shouldNotify() {
        return true;
    }

    public Operation getBackupOperation() {
        final Record record = recordStore.getRecord(dataKey);
        final RecordInfo replicationInfo = Records.buildRecordInfo(record);
        return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, true);
    }

    public void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup && recordStore.getRecord(dataKey) != null;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
        out.writeUTF(ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
        ownerUuid = in.readUTF();

    }
}
