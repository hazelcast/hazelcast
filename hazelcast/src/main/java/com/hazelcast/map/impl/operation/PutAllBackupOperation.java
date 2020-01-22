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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutAllBackupOperation extends MapOperation
        implements PartitionAwareOperation, BackupOperation {

    private boolean disableWanReplicationEvent;
    private List dataKeyDataValueRecord;

    private transient int lastIndex;
    private transient List dataKeyRecord;

    public PutAllBackupOperation(String name,
                                 List dataKeyDataValueRecord,
                                 boolean disableWanReplicationEvent) {
        super(name);
        this.dataKeyDataValueRecord = dataKeyDataValueRecord;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public PutAllBackupOperation() {
    }

    @Override
    protected void runInternal() {
        if (dataKeyRecord != null) {
            for (int i = lastIndex; i < dataKeyRecord.size(); i += 2) {
                Data key = (Data) dataKeyRecord.get(i);
                Record record = (Record) dataKeyRecord.get(i + 1);
                putBackup(key, record);
                lastIndex = i;
            }
        } else {
            // If dataKeyRecord is null and we are in
            // `runInternal` method, means this operation
            // has not been serialized/deserialized
            // and is running directly on caller node
            for (int i = lastIndex; i < dataKeyDataValueRecord.size(); i += 3) {
                Data key = (Data) dataKeyDataValueRecord.get(i);
                Record record = (Record) dataKeyDataValueRecord.get(i + 2);
                putBackup(key, record);
                lastIndex = i;
            }
        }
    }

    private void putBackup(Data key, Record record) {
        Record currentRecord = recordStore.putBackup(key, record,
                false, getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
        publishWanUpdate(key, record.getValue());
        evict(key);
    }

    @Override
    protected boolean disableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(dataKeyDataValueRecord.size() / 3);
        for (int i = 0; i < dataKeyDataValueRecord.size(); i += 3) {
            Data dataKey = (Data) dataKeyDataValueRecord.get(i);
            Data dataValue = (Data) dataKeyDataValueRecord.get(i + 1);
            Record record = (Record) dataKeyDataValueRecord.get(i + 2);

            IOUtil.writeData(out, dataKey);
            Records.writeRecord(out, record, dataValue);
        }
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();
        List dataKeyRecord = new ArrayList<>(size * 2);
        for (int i = 0; i < size; i++) {
            dataKeyRecord.add(IOUtil.readData(in));
            dataKeyRecord.add(Records.readRecord(in));
        }
        this.dataKeyRecord = dataKeyRecord;
        this.disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
