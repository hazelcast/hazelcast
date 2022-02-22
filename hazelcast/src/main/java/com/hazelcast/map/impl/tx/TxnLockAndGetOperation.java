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
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

/**
 * Transactional lock and get operation.
 */
public class TxnLockAndGetOperation
        extends LockAwareOperation implements MutatingOperation {

    private long ttl;
    private boolean shouldLoad;
    private boolean blockReads;
    private UUID ownerUuid;
    private VersionedValue response;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(String name, Data dataKey, long timeout,
                                  long ttl, UUID ownerUuid, boolean shouldLoad,
                                  boolean blockReads) {
        super(name, dataKey);
        this.ownerUuid = ownerUuid;
        this.shouldLoad = shouldLoad;
        this.blockReads = blockReads;
        this.ttl = ttl;
        setWaitTimeout(timeout);
    }

    @Override
    protected void runInternal() {
        if (!recordStore.txnLock(getKey(), ownerUuid, getThreadId(), getCallId(), ttl, blockReads)) {
            throw new TransactionException("Transaction couldn't obtain lock.");
        }
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null && shouldLoad) {
            record = recordStore.loadRecordOrNull(dataKey, false, getCallerAddress());
        }
        Data value = record == null ? null : mapServiceContext.toData(record.getValue());
        response = new VersionedValue(value, record == null ? 0 : record.getVersion());
    }

    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, ownerUuid, getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
        out.writeBoolean(shouldLoad);
        out.writeBoolean(blockReads);
        out.writeLong(ttl);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = UUIDSerializationUtil.readUUID(in);
        shouldLoad = in.readBoolean();
        blockReads = in.readBoolean();
        ttl = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", thread=").append(getThreadId());
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_LOCK_AND_GET;
    }
}
