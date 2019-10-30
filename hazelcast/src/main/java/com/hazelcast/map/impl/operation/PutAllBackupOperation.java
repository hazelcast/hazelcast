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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutAllBackupOperation extends MapOperation
        implements PartitionAwareOperation, BackupOperation {

    private boolean disableWanReplicationEvent;
    private List recordAndDataValuePairs;

    private transient List<Record> dataRecords;

    public PutAllBackupOperation(String name,
                                 List recordAndDataValuePairs,
                                 boolean disableWanReplicationEvent) {
        super(name);
        this.recordAndDataValuePairs = recordAndDataValuePairs;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public PutAllBackupOperation() {
    }

    @Override
    protected void runInternal() {
        if (dataRecords != null) {
            for (Record<Data> record : dataRecords) {
                putBackup(record);
            }
        } else {
            // If dataRecords is null and we are in
            // `runInternal` method, means this operation
            // has not been serialized/deserialized
            // and is running directly on caller node.
            for (int i = 0; i < recordAndDataValuePairs.size(); i += 2) {
                putBackup((Record) recordAndDataValuePairs.get(i));
            }
        }
    }

    private void putBackup(Record record) {
        Record currentRecord = recordStore.putBackup(record, false, getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
        publishWanUpdate(record.getKey(), record.getValue());
        evict(record.getKey());
    }

    @Override
    protected boolean disableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(recordAndDataValuePairs.size() / 2);
        for (int i = 0; i < recordAndDataValuePairs.size(); i += 2) {
            Record record = (Record) recordAndDataValuePairs.get(i);
            Data dataValue = (Data) recordAndDataValuePairs.get(i + 1);
            Records.writeRecord(out, record, dataValue);
        }
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();
        List<Record> dataRecords = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            dataRecords.add(Records.readRecord(in));
        }
        this.dataRecords = dataRecords;
        this.disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
