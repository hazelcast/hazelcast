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
import com.hazelcast.replicatedmap.impl.PartitionContainer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ReplicationOperation extends AbstractOperation {

    private SerializationService serializationService;
    private Map<String, Set<RecordMigrationInfo>> data;

    public ReplicationOperation() {
    }

    public ReplicationOperation(SerializationService serializationService, PartitionContainer container, int partitionId) {
        this.serializationService = serializationService;
        this.setPartitionId(partitionId);
        fetchReplicatedMapRecords(container);
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        if (data == null) {
            return;
        }
        fillRecordStoreWithRecords(service);
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    private void fetchReplicatedMapRecords(PartitionContainer container) {
        data = new HashMap<String, Set<RecordMigrationInfo>>(container.getStores().size());
        for (Map.Entry<String, ReplicatedRecordStore> entry : container.getStores().entrySet()) {
            String name = entry.getKey();
            ReplicatedRecordStore store = entry.getValue();
            Set<RecordMigrationInfo> recordSet = new HashSet<RecordMigrationInfo>(store.size());
            Iterator<ReplicatedRecord> iterator = store.recordIterator();
            while (iterator.hasNext()) {
                ReplicatedRecord record = iterator.next();
                Data dataKey = serializationService.toData(record.getKeyInternal());
                Data dataValue = serializationService.toData(record.getValueInternal());
                recordSet.add(new RecordMigrationInfo(dataKey, dataValue, record.getTtlMillis()));
            }
            data.put(name, recordSet);
        }
    }

    private void fillRecordStoreWithRecords(ReplicatedMapService service) {
        for (Map.Entry<String, Set<RecordMigrationInfo>> dataEntry : data.entrySet()) {
            Set<RecordMigrationInfo> recordSet = dataEntry.getValue();
            String name = dataEntry.getKey();
            ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
            for (RecordMigrationInfo record : recordSet) {
                store.putRecord(record);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Map.Entry<String, Set<RecordMigrationInfo>> entry : data.entrySet()) {
            out.writeUTF(entry.getKey());
            Set<RecordMigrationInfo> recordSet = entry.getValue();
            out.writeInt(recordSet.size());
            for (RecordMigrationInfo record : recordSet) {
                record.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = new HashMap<String, Set<RecordMigrationInfo>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Set<RecordMigrationInfo> recordSet = new HashSet<RecordMigrationInfo>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                RecordMigrationInfo record = new RecordMigrationInfo();
                record.readData(in);
                recordSet.add(record);
            }
            data.put(name, recordSet);
        }
    }

    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }

}
