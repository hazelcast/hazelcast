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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
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

/**
 * Carries all the partition data for replicated map from old owner to the new owner.
 */
public class ReplicationOperation extends AbstractOperation {

    private static ILogger logger = Logger.getLogger(ReplicationOperation.class.getName());

    private SerializationService serializationService;
    private Map<String, Set<RecordMigrationInfo>> data;
    private Map<String, Long> versions;

    public ReplicationOperation() {
    }

    public ReplicationOperation(SerializationService serializationService, PartitionContainer container, int partitionId) {
        this.serializationService = serializationService;
        this.setPartitionId(partitionId);
        fetchReplicatedMapRecords(container);
    }

    @Override
    public void run() throws Exception {
        if (logger.isFinestEnabled()) {
            logger.finest("Moving partition -> " + getPartitionId()
                    + " to the new owner -> " + getNodeEngine().getThisAddress()
                    + " from -> " + getCallerAddress());
        }
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
        int storeCount = container.getStores().size();
        data = new HashMap<String, Set<RecordMigrationInfo>>(storeCount);
        versions = new HashMap<String, Long>(storeCount);
        for (Map.Entry<String, ReplicatedRecordStore> entry : container.getStores().entrySet()) {
            String name = entry.getKey();
            ReplicatedRecordStore store = entry.getValue();
            Set<RecordMigrationInfo> recordSet = new HashSet<RecordMigrationInfo>(store.size());
            Iterator<ReplicatedRecord> iterator = store.recordIterator();
            while (iterator.hasNext()) {
                ReplicatedRecord record = iterator.next();
                Data dataKey = serializationService.toData(record.getKeyInternal());
                Data dataValue = serializationService.toData(record.getValueInternal());
                RecordMigrationInfo migrationInfo = new RecordMigrationInfo();
                migrationInfo.setKey(dataKey);
                migrationInfo.setValue(dataValue);
                migrationInfo.setTtl(record.getTtlMillis());
                migrationInfo.setHits(record.getHits());
                migrationInfo.setCreationTime(record.getCreationTime());
                migrationInfo.setLastAccessTime(record.getLastAccessTime());
                migrationInfo.setLastUpdateTime(record.getUpdateTime());
                recordSet.add(migrationInfo);
            }
            data.put(name, recordSet);
            versions.put(name, store.getVersion());
        }
    }

    private void fillRecordStoreWithRecords(ReplicatedMapService service) {
        for (Map.Entry<String, Set<RecordMigrationInfo>> dataEntry : data.entrySet()) {
            Set<RecordMigrationInfo> recordSet = dataEntry.getValue();
            String name = dataEntry.getKey();
            ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
            long version = versions.get(name);
            for (RecordMigrationInfo record : recordSet) {
                store.putRecord(record);
            }
            store.setVersion(version);
            store.setLoaded(true);
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
        out.writeInt(versions.size());
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
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
        int versionsSize = in.readInt();
        versions = new HashMap<String, Long>(versionsSize);
        for (int i = 0; i < versionsSize; i++) {
            String name = in.readUTF();
            long version = in.readLong();
            versions.put(name, version);
        }
    }


    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }

}
