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

import java.io.IOException;

public class PutBackupOperation
        extends MapOperation implements BackupOperation {

    protected Record<Data> record;
    private Data dataValue;

    public PutBackupOperation(String name, Record<Data> record, Data dataValue) {
        super(name);
        this.record = record;
        this.dataValue = dataValue;
    }

    public PutBackupOperation() {
    }

    @Override
    protected void runInternal() {
        // TODO performance: we can put this record directly into record-store if memory format is BINARY
        Record currentRecord = recordStore.putBackup(record, isPutTransient(), getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
    }

    protected boolean isPutTransient() {
        return false;
    }

    @Override
    protected void afterRunInternal() {
        evict(record.getKey());
        publishWanUpdate(record.getKey(), record.getValue());

        super.afterRunInternal();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        Records.writeRecord(out, record, dataValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        record = Records.readRecord(in);
    }
}
