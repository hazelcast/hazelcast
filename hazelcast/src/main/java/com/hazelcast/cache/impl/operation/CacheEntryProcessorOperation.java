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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

/**
 * Operation of the Cache Entry Processor.
 * <p>{@link javax.cache.processor.EntryProcessor} is executed on the partition.
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} provides the required functionality and this
 * operation is responsible for parameter passing and handling the backup at the end.</p>
 */
public class CacheEntryProcessorOperation
        extends AbstractMutatingCacheOperation {

    private EntryProcessor entryProcessor;
    private Object[] arguments;

    private transient CacheRecord backupRecord;

    public CacheEntryProcessorOperation() {
    }

    public CacheEntryProcessorOperation(String name, Data key, int completionId,
                                        javax.cache.processor.EntryProcessor entryProcessor, Object... arguments) {
        super(name, key, completionId);
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
        // TODO: Add sub-interface to backup using same entry processor instead of put backup
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.ENTRY_PROCESSOR;
    }

    @Override
    public void run()
            throws Exception {
        response = cache.invoke(key, entryProcessor, arguments);
        backupRecord = cache.getRecord(key);
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

}
