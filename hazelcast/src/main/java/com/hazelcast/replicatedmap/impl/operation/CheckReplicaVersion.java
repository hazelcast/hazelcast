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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CheckReplicaVersion extends AbstractOperation implements PartitionAwareOperation {

    private String name;
    private long version;
    private boolean response;

    public CheckReplicaVersion() {
    }

    public CheckReplicaVersion(String name, long version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, getPartitionId());
        if (store == null) {
            return;
        }
        long currentVersion = store.getPartitionVersion();
        if (currentVersion == version) {
            response = true;
        } else {
            response = false;
            Set<RecordMigrationInfo> recordSet = getRecordSet(store);
            SyncReplicatedMapDataOperation op = new SyncReplicatedMapDataOperation(name, recordSet);
            op.setPartitionId(getPartitionId());
            op.setValidateTarget(false);
            OperationService operationService = getNodeEngine().getOperationService();
            operationService.invokeOnTarget(ReplicatedMapService.SERVICE_NAME, op, getCallerAddress());
        }
    }

    private Set<RecordMigrationInfo> getRecordSet(ReplicatedRecordStore store) {
        Set<RecordMigrationInfo> recordSet = new HashSet<RecordMigrationInfo>(store.size());
        Iterator<ReplicatedRecord> iterator = store.recordIterator();
        while (iterator.hasNext()) {
            ReplicatedRecord record = iterator.next();
            SerializationService serializationService = getNodeEngine().getSerializationService();
            Data dataKey = serializationService.toData(record.getKeyInternal());
            Data dataValue = serializationService.toData(record.getValueInternal());
            recordSet.add(new RecordMigrationInfo(dataKey, dataValue, record.getTtlMillis()));
        }
        return recordSet;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        version = in.readLong();
    }
}
