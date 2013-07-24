/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.*;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordReplicationInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.util.Clock;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author mdogan 7/24/12
 */
public class MapReplicationOperation extends AbstractOperation {

    private Map<String, Set<RecordReplicationInfo>> data;

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(PartitionContainer container, int partitionId, int replicaIndex) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        data = new HashMap<String, Set<RecordReplicationInfo>>(container.getMaps().size());
        for (Entry<String, PartitionRecordStore> entry : container.getMaps().entrySet()) {
            String name = entry.getKey();
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            final MapConfig mapConfig = entry.getValue().getMapContainer().getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            Set<RecordReplicationInfo> recordSet = new HashSet<RecordReplicationInfo>(recordStore.getRecords().size());
            for (Entry<Data, Record> recordEntry : recordStore.getRecords().entrySet()) {
                Data key = recordEntry.getKey();
                Record record = recordEntry.getValue();
                if (record.getValue() == null) {
                    // see optimization at PartitionRecordStore.get(Data dataKey)
                    continue;
                }
                RecordReplicationInfo recordReplicationInfo = null;
                if(replicaIndex == 0) {
                    recordReplicationInfo = createScheduledRecordState(mapContainer, recordEntry, key);
                }
                else {
                    recordReplicationInfo = new RecordReplicationInfo(record);
                }
                recordSet.add(recordReplicationInfo);
            }
            data.put(name, recordSet);
        }
    }

    private RecordReplicationInfo createScheduledRecordState(MapContainer mapContainer, Entry<Data, Record> recordEntry, Data key) {
        ScheduledEntry idleScheduledEntry = mapContainer.getIdleEvictionScheduler() == null ? null : mapContainer.getIdleEvictionScheduler().cancel(key);
        long idleDelay = idleScheduledEntry == null ? -1 : findDelayMillis(idleScheduledEntry);

        ScheduledEntry ttlScheduledEntry = mapContainer.getTtlEvictionScheduler() == null ? null : mapContainer.getTtlEvictionScheduler().cancel(key);
        long ttlDelay = ttlScheduledEntry == null ? -1 : findDelayMillis(ttlScheduledEntry);

        ScheduledEntry writeScheduledEntry = mapContainer.getMapStoreWriteScheduler() == null ? null : mapContainer.getMapStoreWriteScheduler().cancel(key);
        long writeDelay = writeScheduledEntry == null ? -1 : findDelayMillis(writeScheduledEntry);

        ScheduledEntry deleteScheduledEntry = mapContainer.getMapStoreDeleteScheduler() == null ? null : mapContainer.getMapStoreDeleteScheduler().cancel(key);
        long deleteDelay = deleteScheduledEntry == null ? -1 : findDelayMillis(deleteScheduledEntry);

        return new RecordReplicationInfo(recordEntry.getValue(), idleDelay, ttlDelay, writeDelay, deleteDelay);
    }

    public void run() {
        MapService mapService = getService();
        if (data != null) {
            for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    Record inputRecord = recordReplicationInfo.getRecord();
                    Data key = inputRecord.getKey();
                    Record record = mapService.createRecord(mapName, key, inputRecord.getValue(), -1, false);
                    record.setStatistics(inputRecord.getStatistics());
                    recordStore.getRecords().put(key, record);
                    if(recordReplicationInfo.getIdleDelayMillis() >= 0) {
                        mapService.scheduleIdleEviction(mapName, key, recordReplicationInfo.getIdleDelayMillis());
                    }
                    if(recordReplicationInfo.getTtlDelayMillis() >= 0) {
                        mapService.scheduleTtlEviction(mapName, record, recordReplicationInfo.getTtlDelayMillis());
                    }
                    if(recordReplicationInfo.getMapStoreWriteDelayMillis() >= 0) {
                        mapService.scheduleMapStoreWrite(mapName, key, record.getValue(), recordReplicationInfo.getMapStoreWriteDelayMillis());
                    }
                    if(recordReplicationInfo.getMapStoreDeleteDelayMillis() >= 0) {
                        mapService.scheduleMapStoreDelete(mapName, key, recordReplicationInfo.getMapStoreDeleteDelayMillis());
                    }
                }
            }
        }
    }

    private long findDelayMillis(ScheduledEntry entry) {
        return Math.max(0, entry.getScheduledDelayMillis() - (Clock.currentTimeMillis() - entry.getScheduleTime()));
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    protected void readInternal(final ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = new HashMap<String, Set<RecordReplicationInfo>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Set<RecordReplicationInfo> recordReplicationInfos = new HashSet<RecordReplicationInfo>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                RecordReplicationInfo recordReplicationInfo = in.readObject();
                recordReplicationInfos.add(recordReplicationInfo);
            }
            data.put(name, recordReplicationInfos);
        }
    }

    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Entry<String, Set<RecordReplicationInfo>> mapEntry : data.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Set<RecordReplicationInfo> recordReplicationInfos = mapEntry.getValue();
            out.writeInt(recordReplicationInfos.size());
            for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                out.writeObject(recordReplicationInfo);
            }
        }
    }

    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }
}
