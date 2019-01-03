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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class PutAllBackupOperation extends MapOperation
        implements PartitionAwareOperation, BackupOperation, Versioned {

    private MapEntries entries;
    private List<RecordInfo> recordInfos;

    public PutAllBackupOperation(String name, MapEntries entries,
                                 List<RecordInfo> recordInfos, boolean disableWanReplicationEvent) {
        super(name);
        this.entries = entries;
        this.recordInfos = recordInfos;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public PutAllBackupOperation() {
    }

    @Override
    public void run() {
        for (int i = 0; i < entries.size(); i++) {
            Data dataKey = entries.getKey(i);
            Data dataValue = entries.getValue(i);
            Record record = recordStore.putBackup(dataKey, dataValue, getCallerProvenance());
            applyRecordInfo(record, recordInfos.get(i));

            publishWanUpdate(dataKey, dataValue);
            evict(dataKey);
        }
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        entries.writeData(out);
        for (RecordInfo recordInfo : recordInfos) {
            recordInfo.writeData(out);
        }

        // RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            out.writeBoolean(disableWanReplicationEvent);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        entries = new MapEntries();
        entries.readData(in);
        int size = entries.size();
        recordInfos = new ArrayList<RecordInfo>(size);
        for (int i = 0; i < size; i++) {
            RecordInfo recordInfo = new RecordInfo();
            recordInfo.readData(in);
            recordInfos.add(recordInfo);
        }

        // RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            disableWanReplicationEvent = in.readBoolean();
        }
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
