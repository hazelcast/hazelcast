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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.BackupAwareEntryProcessor;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

/**
 * Operation of the Cache Entry Processor.
 * <p>{@link javax.cache.processor.EntryProcessor} is executed on the partition.
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} provides the required functionality and this
 * operation is responsible for parameter passing and handling the backup at the end.</p>
 */
public class CacheEntryProcessorOperation
        extends MutatingCacheOperation {

    private EntryProcessor entryProcessor;
    private Object[] arguments;

    private transient CacheRecord backupRecord;
    private transient EntryProcessor backupEntryProcessor;

    public CacheEntryProcessorOperation() {
    }

    public CacheEntryProcessorOperation(String cacheNameWithPrefix, Data key, int completionId,
                                        javax.cache.processor.EntryProcessor entryProcessor, Object... arguments) {
        super(cacheNameWithPrefix, key, completionId);
        this.entryProcessor = entryProcessor;
        this.arguments = arguments;
        this.completionId = completionId;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        if (backupEntryProcessor != null) {
            return new CacheBackupEntryProcessorOperation(name, key, backupEntryProcessor, arguments);
        } else {
            if (backupRecord != null) {
                // After entry processor is executed if there is a record, this means that possible add/update
                return new CachePutBackupOperation(name, key, backupRecord);
            } else {
                // If there is no record, this means possible remove by entry processor.
                // TODO In case of non-existing key, this cause redundant remove operation to backups
                // Better solution may be using a new interface like "EntryProcessorListener" on "invoke" method
                // for handling add/update/remove cases properly at execution of "EntryProcessor".
                return new CacheRemoveBackupOperation(name, key);
            }
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.ENTRY_PROCESSOR;
    }

    @Override
    public void run()
            throws Exception {
        response = recordStore.invoke(key, entryProcessor, arguments, completionId);
        if (entryProcessor instanceof BackupAwareEntryProcessor) {
            BackupAwareEntryProcessor processor = (BackupAwareEntryProcessor) entryProcessor;
            backupEntryProcessor = processor.createBackupEntryProcessor();
        }
        if (backupEntryProcessor == null) {
            backupRecord = recordStore.getRecord(key);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (recordStore.isWanReplicationEnabled()) {
            CacheRecord record = recordStore.getRecord(key);
            if (record != null) {
                publishWanUpdate(key, record);
            } else {
                publishWanRemove(key);
            }
        }
        super.afterRun();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeBoolean(arguments != null);
        if (arguments != null) {
            out.writeInt(arguments.length);
            for (Object arg : arguments) {
                out.writeObject(arg);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        final boolean hasArguments = in.readBoolean();
        if (hasArguments) {
            final int size = in.readInt();
            arguments = new Object[size];
            for (int i = 0; i < size; i++) {
                arguments[i] = in.readObject();
            }
        }
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
