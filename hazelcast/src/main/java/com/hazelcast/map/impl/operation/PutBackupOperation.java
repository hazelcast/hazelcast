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
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class PutBackupOperation extends MapOperation implements BackupOperation {

    protected Data dataKey;
    protected Data dataValue;
    protected RecordInfo recordInfo;

    public PutBackupOperation(String name, Data dataKey, Data dataValue,
                              RecordInfo recordInfo) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.recordInfo = recordInfo;
    }

    public PutBackupOperation() {
    }

    @Override
    protected void runInternal() {
        Record record = recordStore.putBackup(dataKey, dataValue, recordInfo.getTtl(),
                recordInfo.getMaxIdle(), isPutTransient(), getCallerProvenance());

        applyRecordInfo(record, recordInfo);
    }

    protected boolean isPutTransient() {
        return false;
    }

    @Override
    protected void afterRunInternal() {
        evict(dataKey);
        publishWanUpdate(dataKey, dataValue);

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

        out.writeData(dataKey);
        out.writeData(dataValue);
        recordInfo.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        dataKey = in.readData();
        dataValue = in.readData();
        recordInfo = new RecordInfo();
        recordInfo.readData(in);
    }
}
