/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.exception.WrongTargetException;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.internal.util.TimeUtil.zeroOutMs;

/**
 * Used to transfer expired keys from owner replica to backup replicas.
 */
public class EvictBatchBackupOperation extends MapOperation implements BackupOperation {

    private int primaryEntryCount;
    private String name;
    private Collection<ExpiredKey> expiredKeys;

    public EvictBatchBackupOperation() {
    }

    public EvictBatchBackupOperation(String name, Collection<ExpiredKey> expiredKeys, int primaryEntryCount) {
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
            Record existingRecord = recordStore.getRecord(expiredKey.getKey());
            if (canEvictRecord(existingRecord, expiredKey)) {
                recordStore.evict(existingRecord.getKey(), true);
            }
        }

        equalizeEntryCountWithPrimary();
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

    private boolean canEvictRecord(Record existingRecord, ExpiredKey expiredKey) {
        if (existingRecord == null) {
            return false;
        }

        // creation time of a record is always same between all replicas.
        // by doing creation time check we can prevent un-wanted record deletion on replicas.
        // un-wanted record deletion example: on primary record was expired and queued but before
        // we send it to backups a new record is added with same key, when we send queued item
        // to backups, backups should not remove it. Comparing creation times to be sure that
        // we are deleting correct record.
        // since 3.11, creationTime is maintained at second accuracy
        return existingRecord.getCreationTime() == zeroOutMs(expiredKey.getCreationTime());
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVICT_BATCH_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(name);
        out.writeInt(expiredKeys.size());
        for (ExpiredKey expiredKey : expiredKeys) {
            out.writeData(expiredKey.getKey());
            out.writeLong(expiredKey.getCreationTime());
        }
        out.writeInt(primaryEntryCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        name = in.readUTF();
        int size = in.readInt();
        expiredKeys = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            expiredKeys.add(new ExpiredKey(in.readData(), in.readLong()));
        }
        primaryEntryCount = in.readInt();
    }
}
