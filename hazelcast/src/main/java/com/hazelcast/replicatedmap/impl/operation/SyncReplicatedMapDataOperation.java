/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SyncReplicatedMapDataOperation extends AbstractOperation {

    private String name;
    private Set<RecordMigrationInfo> recordSet;

    public SyncReplicatedMapDataOperation() {
    }

    public SyncReplicatedMapDataOperation(String name, Set<RecordMigrationInfo> recordSet) {
        this.name = name;
        this.recordSet = recordSet;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        store.clear(false);
        for (RecordMigrationInfo record : recordSet) {
            store.putRecord(record);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(recordSet.size());
        for (RecordMigrationInfo record : recordSet) {
            record.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        int size = in.readInt();
        recordSet = new HashSet<RecordMigrationInfo>(size);
        for (int j = 0; j < size; j++) {
            RecordMigrationInfo record = new RecordMigrationInfo();
            record.readData(in);
            recordSet.add(record);
        }
    }
}
