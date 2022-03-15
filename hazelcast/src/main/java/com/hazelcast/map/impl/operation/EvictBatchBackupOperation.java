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

import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Used to transfer expired keys from owner replica to backup replicas.
 */
public class EvictBatchBackupOperation
        extends MapOperation implements BackupOperation {

    private int primaryEntryCount;
    private String name;
    private Collection<ExpiredKey> expiredKeys;

    public EvictBatchBackupOperation() {
    }

    public EvictBatchBackupOperation(String name,
                                     Collection<ExpiredKey> expiredKeys,
                                     int primaryEntryCount) {
        super(name);

        assert name != null;

        this.name = name;
        this.expiredKeys = expiredKeys;
        this.primaryEntryCount = primaryEntryCount;
        this.createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }

        for (ExpiredKey expiredKey : expiredKeys) {
            Data key = expiredKey.getKey();
            Record existingRecord = recordStore.getRecord(key);
            if (hasSameValueHashCode(existingRecord, expiredKey)) {
                recordStore.evict(key, true);
            }
        }

        equalizeEntryCountWithPrimary();
    }

     public boolean hasSameValueHashCode(Record existingRecord, ExpiredKey expiredKey) {
        if (existingRecord == null) {
            return false;
        }

        // Value-hash-code = Hash code of value
        // returned from method record#getValue()
        //
        // Value-hash-code of a record is always same between all
        // replicas. By doing value-hash-code comparison we can prevent
        // un-wanted record deletion on backup replicas. This is the
        // scenario we tried to address here: Let's say on primary
        // replica a record was expired and then it was queued to be
        // sent to backup replicas but before it was sent to backup
        // replicas, a new record was added to IMap with the same
        // key, in this scenario, when already queued item is sent
        // to backup replicas, backups should not remove the key if
        // value-hash-codes are not equal. This will help to decrease
        // possibility of un-wanted record deletion on backup replicas.
        return existingRecord.getValue().hashCode() == expiredKey.getMetadata();
    }

    /**
     * Equalizes backup entry count with primary in order to have identical
     * memory occupancy.
     *
     * If eviction configured for this map, equalize entry count by using
     * evictor, otherwise, sample entries and evict them from this backup
     * replica.
     */
    private void equalizeEntryCountWithPrimary() {
        int diff = recordStore.size() - primaryEntryCount;
        if (diff <= 0) {
            return;
        }

        recordStore.sampleAndForceRemoveEntries(diff);

        assert recordStore.size() == primaryEntryCount : String.format("Failed"
                        + " to remove %d entries while attempting to match"
                        + " primary entry count %d,"
                        + " recordStore size is now %d",
                diff, primaryEntryCount, recordStore.size());
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

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVICT_BATCH_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeString(name);
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

        name = in.readString();
        int size = in.readInt();
        expiredKeys = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            expiredKeys.add(new ExpiredKey(IOUtil.readData(in), in.readLong()));
        }
        primaryEntryCount = in.readInt();
    }
}
