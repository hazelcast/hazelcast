/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenericBackupOperation extends AbstractNamedKeyBasedOperation
        implements Comparable<GenericBackupOperation>, BackupOperation {

    enum BackupOpType {
        PUT,
        REMOVE,
        LOCK,
        UNLOCK
    }

    Data dataValue = null;
    long ttl = LockOperation.DEFAULT_LOCK_TTL; // how long should the lock live?
    BackupOpType backupOpType = BackupOpType.PUT;
    Address firstCallerAddress = null;
    long firstCallerId = -1;
    boolean invocation = false;
    long version = 0;

    public GenericBackupOperation(String name, Data dataKey, Data dataValue, long ttl, long version) {
        super(name, dataKey);
        this.ttl = ttl;
        this.dataValue = dataValue;
        this.version = version;
    }

    public GenericBackupOperation(String name, BackupAwareOperation op, long version) {
        super(name, op.getKey());
        this.ttl = op.ttl;
        this.dataValue = op.getValue();
        this.version = version;
    }

    public GenericBackupOperation() {
    }

    public void setBackupOpType(BackupOpType backupOpType) {
        this.backupOpType = backupOpType;
    }

    public void setFirstCallerId(long firstCallerId, Address firstCallerAddress) {
        this.firstCallerId = firstCallerId;
        this.firstCallerAddress = firstCallerAddress;
    }

    public void run() {
        MapService mapService = (MapService) getService();
        mapService.getPartitionContainer(getPartitionId()).handleBackupOperation(this);
    }

    void backupAndReturn() {
        doBackup();
        sendResponse();
    }

    void doBackup() {
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        Address caller = getCaller();
        MapPartition mapPartition = mapService.getMapPartition(partitionId, name);
        if (backupOpType == BackupOpType.PUT) {
            Record record = mapPartition.records.get(dataKey);
            if (record == null) {
                record = new DefaultRecord(mapPartition.partitionInfo.getPartitionId(), dataKey, dataValue, -1, -1,
                        mapService.nextId());
                mapPartition.records.put(dataKey, record);
            } else {
                record.setValueData(dataValue);
            }
            record.setActive();
            record.setDirty(true);
        } else if (backupOpType == BackupOpType.REMOVE) {
            Record record = mapPartition.records.remove(dataKey);
            if (record != null) {
                record.markRemoved();
            }
        } else {
            if (backupOpType == BackupOpType.LOCK) {
                LockInfo lock = mapPartition.getOrCreateLock(getKey());
                lock.lock(caller, threadId, ttl);
            } else if (backupOpType == BackupOpType.UNLOCK) {
                LockInfo lock = mapPartition.getLock(getKey());
                if (lock != null) {
                    lock.unlock(caller, threadId);
                }
                lock.lock(caller, threadId, ttl);
            }
        }
    }

    void sendResponse() {
        if (invocation) {
//            getResponseHandler().sendResponse(Boolean.TRUE);
        } else {
            int partitionId = getPartitionId();
            final AsyncBackupResponse backupResponse = new AsyncBackupResponse();
            backupResponse.setServiceName(MapService.MAP_SERVICE_NAME).setCallId(firstCallerId)
                    .setPartitionId(partitionId).setReplicaIndex(0);
            getNodeService().send(backupResponse, partitionId, firstCallerAddress);
        }
    }

    public int compareTo(GenericBackupOperation another) {
        long thisVal = this.version;
        long anotherVal = another.version;
        return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, dataValue);
        out.writeLong(ttl);
        out.writeInt(backupOpType.ordinal());
        out.writeLong(firstCallerId);
        boolean NULL = (firstCallerAddress == null);
        out.writeBoolean(NULL);
        if (!NULL) {
            firstCallerAddress.writeData(out);
        }
        out.writeBoolean(invocation);
        out.writeLong(version);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        dataValue = IOUtil.readNullableData(in);
        ttl = in.readLong();
        backupOpType = BackupOpType.values()[in.readInt()];
        firstCallerId = in.readLong();
        boolean NULL = in.readBoolean();
        if (!NULL) {
            firstCallerAddress = new Address();
            firstCallerAddress.readData(in);
        }
        invocation = in.readBoolean();
        version = in.readLong();
    }

    public void setInvocation(boolean invocation) {
        this.invocation = invocation;
    }
}