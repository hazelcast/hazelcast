/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * Set & Unlock processing for the EntryOperation
 *
 * See the javadoc on {@link EntryOperation}
 */
public class EntryOffloadableSetUnlockOperation extends KeyBasedMapOperation
        implements BackupAwareOperation, Notifier {

    protected Data newValue;
    protected Data oldValue;
    protected String caller;
    protected long begin;
    protected EntryEventType modificationType;
    protected EntryBackupProcessor entryBackupProcessor;

    public EntryOffloadableSetUnlockOperation() {
    }

    public EntryOffloadableSetUnlockOperation(String name, EntryEventType modificationType, Data key, Data oldValue,
                                              Data newValue, String caller, long threadId, long begin,
                                              EntryBackupProcessor entryBackupProcessor) {
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
    public void run() throws Exception {
        verifyLock();
        try {
            operator(this).init(dataKey, oldValue, newValue, null, modificationType)
                    .doPostOperateOps();
        } finally {
            unlockKey();
        }
    }

    private void verifyLock() {
        if (!recordStore.isLockedBy(dataKey, caller, threadId)) {
            // we can't send a RetryableHazelcastException explicitly since it would retry this opertation and we want to retry
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
        return entryBackupProcessor != null ? new EntryBackupOperation(name, dataKey, entryBackupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryBackupProcessor != null;
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
    public int getId() {
        return MapDataSerializerHook.ENTRY_OFFLOADABLE_SET_UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(modificationType != null ? modificationType.name() : "");
        out.writeData(oldValue);
        out.writeData(newValue);
        out.writeUTF(caller);
        out.writeLong(begin);
        out.writeObject(entryBackupProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        String modificationTypeName = in.readUTF();
        modificationType = modificationTypeName.equals("") ? null : EntryEventType.valueOf(modificationTypeName);
        oldValue = in.readData();
        newValue = in.readData();
        caller = in.readUTF();
        begin = in.readLong();
        entryBackupProcessor = in.readObject();
    }

}
