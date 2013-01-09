/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.IOException;

public class PutBackupOperation extends AbstractNamedKeyBasedOperation implements BackupOperation {

    Data dataValue = null;
    long ttl = -1;

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
        this.dataValue = dataValue;
    }

    public PutBackupOperation() {
    }

    public void run() {
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        RecordStore recordStore = mapService.getRecordStore(partitionId, name);
        Record record = recordStore.getRecords().get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            recordStore.getRecords().put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, dataValue);
        out.writeLong(ttl);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataValue = IOUtil.readNullableData(in);
        ttl = in.readLong();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PutBackupOperation{}";
    }
}
