/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.replicatedmap.impl.PartitionContainer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Carries all the partition data for replicated map from old owner to the new owner.
 */
public class ReplicationOperation extends AbstractSerializableOperation {

    private SerializationService serializationService;
    private Map<String, Set<RecordMigrationInfo>> data;
    private Map<String, Long> versions;

    public ReplicationOperation() {
    }

    public ReplicationOperation(SerializationService serializationService, PartitionContainer container, int partitionId) {
        this.serializationService = serializationService;

        setPartitionId(partitionId);
        fetchReplicatedMapRecords(container);
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Moving replicated map (partitionId " + getPartitionId() + ") from " + getCallerAddress()
                    + " to the new owner " + getNodeEngine().getThisAddress());
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
        data = createHashMap(storeCount);
        versions = createHashMap(storeCount);
        for (Map.Entry<String, ReplicatedRecordStore> entry : container.getStores().entrySet()) {
            String name = entry.getKey();
            ReplicatedRecordStore store = entry.getValue();
            Set<RecordMigrationInfo> recordSet = createHashSet(store.size());
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
            AbstractReplicatedRecordStore store =
                    (AbstractReplicatedRecordStore) service.getReplicatedRecordStore(name, true, getPartitionId());
            long version = versions.get(name);
            store.putRecords(recordSet, version);
            store.setLoaded(true);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Map.Entry<String, Set<RecordMigrationInfo>> entry : data.entrySet()) {
            out.writeString(entry.getKey());
            Set<RecordMigrationInfo> recordSet = entry.getValue();
            out.writeInt(recordSet.size());
            for (RecordMigrationInfo record : recordSet) {
                record.writeData(out);
            }
        }
        out.writeInt(versions.size());
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            int mapSize = in.readInt();
            Set<RecordMigrationInfo> recordSet = createHashSet(mapSize);
            for (int j = 0; j < mapSize; j++) {
                RecordMigrationInfo record = new RecordMigrationInfo();
                record.readData(in);
                recordSet.add(record);
            }
            data.put(name, recordSet);
        }
        int versionsSize = in.readInt();
        versions = createHashMap(versionsSize);
        for (int i = 0; i < versionsSize; i++) {
            String name = in.readString();
            long version = in.readLong();
            versions.put(name, version);
        }
    }

    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.REPLICATION;
    }
}
