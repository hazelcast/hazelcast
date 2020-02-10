/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.map.impl.DirectBackupEntryProcessor.DIRECT_BACKUP_PROCESSOR;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * Set &amp; Unlock processing for the EntryOperation
 *
 * See the javadoc on {@link EntryOperation}
 */
public class EntryOffloadableSetUnlockOperation extends KeyBasedMapOperation
        implements BackupAwareOperation, Notifier {

    protected Data newValue;
    protected Data oldValue;
    protected UUID caller;
    protected long begin;
    protected EntryEventType modificationType;
    protected EntryProcessor entryBackupProcessor;

    private transient Record backupRecord;
    private transient Data backupValue;

    public EntryOffloadableSetUnlockOperation() {
    }

    public EntryOffloadableSetUnlockOperation(String name, EntryEventType modificationType, Data key, Data oldValue,
                                              Data newValue, UUID caller, long threadId, long begin,
                                              EntryProcessor entryBackupProcessor) {
        super(name, key, newValue);
        this.newValue = newValue;
        this.oldValue = oldValue;
        this.caller = caller;
        this.begin = begin;
        this.modificationType = modificationType;
        this.entryBackupProcessor = entryBackupProcessor;
        this.setThreadId(threadId);
    }

    @Override
    protected void runInternal() {
        verifyLock();
        try {
            EntryOperator operator = operator(this);
            operator.init(dataKey, oldValue, newValue, null, modificationType, null)
                    .doPostOperateOps();
            if (shouldUseDirectBackup()) {
                backupRecord = recordStore.getRecord(dataKey);
                backupValue = operator.getValueData();
            }
        } finally {
            unlockKey();
        }
    }

    private void verifyLock() {
        if (!recordStore.isLockedBy(dataKey, caller, threadId)) {
            // we can't send a RetryableHazelcastException explicitly since it would retry this operation and we want to retry
            // the preceding EntryOperation that this operation is part of.
            throw new EntryOffloadableLockMismatchException(
                    String.format("The key is not locked by the caller=%s and threadId=%d", caller, threadId));
        }
    }

    private void unlockKey() {
        boolean unlocked = recordStore.unlock(dataKey, caller, threadId, getCallId());
        if (!unlocked) {
            throw new IllegalStateException(
                    String.format("Unexpected error! EntryOffloadableSetUnlockOperation finished but the unlock method "
                            + "returned false for caller=%s and threadId=%d", caller, threadId));
        }
    }

    @Override
    public boolean returnsResponse() {
        // this has to be true, otherwise the calling side won't be notified about the exception thrown by this operation
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        if (entryBackupProcessor == DIRECT_BACKUP_PROCESSOR) {
            return backupRecord == null ? new RemoveBackupOperation(name, dataKey, false) : new PutBackupOperation(name, dataKey,
                    backupRecord, backupValue);
        }
        return entryBackupProcessor != null ? new EntryBackupOperation(name, dataKey, entryBackupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryBackupProcessor != null;
    }

    private boolean shouldUseDirectBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryBackupProcessor == DIRECT_BACKUP_PROCESSOR;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
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
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ENTRY_OFFLOADABLE_SET_UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(modificationType != null ? modificationType.name() : "");
        IOUtil.writeData(out, oldValue);
        IOUtil.writeData(out, newValue);
        UUIDSerializationUtil.writeUUID(out, caller);
        out.writeLong(begin);
        out.writeObject(entryBackupProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        String modificationTypeName = in.readUTF();
        modificationType = modificationTypeName.equals("") ? null : EntryEventType.valueOf(modificationTypeName);
        oldValue = IOUtil.readData(in);
        newValue = IOUtil.readData(in);
        caller = UUIDSerializationUtil.readUUID(in);
        begin = in.readLong();
        entryBackupProcessor = in.readObject();
    }

}
