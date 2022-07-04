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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.exception.WrongTargetException;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Used to transfer expired keys from owner replica to backup replicas.
 */
public class CacheExpireBatchBackupOperation extends CacheOperation {

    private int primaryEntryCount;
    private Collection<ExpiredKey> expiredKeys;

    public CacheExpireBatchBackupOperation() {
    }

    public CacheExpireBatchBackupOperation(String name, Collection<ExpiredKey> expiredKeys, int primaryEntryCount) {
        super(name, true);
        this.expiredKeys = expiredKeys;
        this.primaryEntryCount = primaryEntryCount;
    }

    @Override
    public void run() {
        if (recordStore == null) {
            return;
        }

        for (ExpiredKey expiredKey : expiredKeys) {
            evictIfSame(expiredKey);
        }

        equalizeEntryCountWithPrimary();
    }

    /**
     * Equalizes backup entry count with primary in order to have identical
     * memory occupancy.
     */
    private void equalizeEntryCountWithPrimary() {
        int diff = recordStore.size() - primaryEntryCount;
        if (diff > 0) {
            recordStore.sampleAndForceRemoveEntries(diff);

            assert recordStore.size() == primaryEntryCount : String.format("Failed"
                            + " to remove %d entries while attempting to match"
                            + " primary entry count %d,"
                            + " recordStore size is now %d",
                    diff, primaryEntryCount, recordStore.size());
        }
    }

    @Override
    public void afterRun() throws Exception {
        try {
            super.afterRun();
        } finally {
            recordStore.disposeDeferredBlocks();
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof WrongTargetException) {
            if (((WrongTargetException) throwable).getTarget() == null) {
                // If there isn't any address of backup replica, no need to retry this operation.
                return ExceptionAction.THROW_EXCEPTION;
            }
        }

        return super.onInvocationException(throwable);
    }

    public void evictIfSame(ExpiredKey key) {
        CacheRecord record = recordStore.getRecord(key.getKey());
        if (record != null && record.getCreationTime() == key.getMetadata()) {
            recordStore.removeRecord(key.getKey());
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.EXPIRE_BATCH_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(expiredKeys.size());
        for (ExpiredKey expiredKey : expiredKeys) {
            IOUtil.writeData(out, expiredKey.getKey());
            out.writeLong(expiredKey.getMetadata());
        }
        out.writeInt(primaryEntryCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        expiredKeys = new LinkedList<ExpiredKey>();
        for (int i = 0; i < size; i++) {
            expiredKeys.add(new ExpiredKey(IOUtil.readData(in), in.readLong()));
        }
        primaryEntryCount = in.readInt();
    }
}
