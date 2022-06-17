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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.UUID;

import static java.lang.String.format;

public final class TSPutBackupOperation extends PutBackupOperation implements BlockingOperation, Notifier {

    private Record status;
    private long threadId;

    private UUID ownerUuid;

    public TSPutBackupOperation(String name, Data dataKey,
                                Record<Data> record, Data dataValue,
                                ExpiryMetadata expiryMetadata) {
        super(name, dataKey, record, dataValue, expiryMetadata);
        this.ownerUuid = UuidUtil.newUnsecureUUID();
    }

    public TSPutBackupOperation() {
        super();
    }

    @Override
    protected void runInternal() {
        final long finalThreadId = getThreadId();
        final long finalCallId = getCallId();

        if (!recordStore.isLockedBy(dataKey, ownerUuid, finalThreadId)) {
            lock(dataKey, ownerUuid, finalThreadId, finalCallId);
        }

        Record currentRecord = recordStore.putBackup(dataKey, record,
            expiryMetadata, isPutTransient(), getCallerProvenance());
        if (isPendingIO(currentRecord)) {
            status = currentRecord;
            // IO offloading is required, the lock should be kept
            return;
        }
        Records.copyMetadataFrom(record, currentRecord);

        unlock(dataKey, ownerUuid, finalThreadId, finalCallId);
    }

    @Override
    public boolean isPendingResult() {
        return isPendingIO(status);
    }

    @Override
    protected Offload newIOOperationOffload() {
        return recordStore.newIOOperationOffload(dataKey, this, status);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TS_PUT_BACKUP;
    }

    private void lock(Data finalDataKey, UUID finalCaller, long finalThreadId, long finalCallId) {
        boolean locked = recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);
        if (!locked) {
            // should not happen since it's a lock-awaiting operation and we are on a partition-thread, but just to make sure
            throw new IllegalStateException(
                format("Could not obtain a lock by the caller=%s and threadId=%d", finalCaller, threadId));
        }
    }

    private void unlock(Data finalDataKey, UUID finalCaller, long finalThreadId, long finalCallId) {
        boolean unlocked = recordStore.unlock(finalDataKey, finalCaller, finalThreadId, finalCallId);
        if (!unlocked) {
            throw new IllegalStateException(
                format("Could not unlock by the caller=%s and threadId=%d", finalCaller, threadId));
        }
    }

    @Override
    public long getThreadId() {
        return threadId;
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.canAcquireLock(dataKey, ownerUuid, threadId)
            || recordStore.isLockedBy(dataKey, ownerUuid, threadId)) {
            // Don't wait if there is no any lock on dataKey or the operation already acquired it
            return false;
        }

        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
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
    public boolean shouldNotify() {
        if (recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            // The operation released the lock
            return true;
        }
        return false;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }
}
