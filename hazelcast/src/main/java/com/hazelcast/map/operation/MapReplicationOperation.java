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
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordReplicationInfo;
import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author mdogan 7/24/12
 */
public class MapReplicationOperation extends AbstractOperation {

    private Map<String, Set<RecordReplicationInfo>> data;
    private Map<String, Boolean> mapInitialLoadInfo;
    private Map<String, List<DelayedEntry>> delayedEntries;

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(MapService mapService, PartitionContainer container, int partitionId, int replicaIndex) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        data = new HashMap<String, Set<RecordReplicationInfo>>(container.getMaps().size());
        mapInitialLoadInfo = new HashMap<String, Boolean>(container.getMaps().size());
        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            final MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            String name = entry.getKey();
            // adding if initial data is loaded for the only maps that has mapstore behind
            if (mapContainer.getStore() != null) {
                mapInitialLoadInfo.put(name, replicaIndex > 0 || recordStore.isLoaded());
            }
            // now prepare data to migrate records
            Set<RecordReplicationInfo> recordSet = new HashSet<RecordReplicationInfo>();
            for (Entry<Data, Record> recordEntry : recordStore.getReadonlyRecordMap().entrySet()) {
                Record record = recordEntry.getValue();
                RecordReplicationInfo recordReplicationInfo;
                recordReplicationInfo = mapService.createRecordReplicationInfo(record);
                recordSet.add(recordReplicationInfo);
            }
            data.put(name, recordSet);
        }
        readDelayedEntries(container);
    }

    private void readDelayedEntries(PartitionContainer container) {
        delayedEntries = new HashMap<String, List<DelayedEntry>>(container.getMaps().size());
        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            final List<DelayedEntry> delayedEntries = recordStore.getWriteBehindQueue().getSnapShot().asList();
            if (delayedEntries != null && delayedEntries.size() == 0) {
                continue;
            }
            this.delayedEntries.put(entry.getKey(), delayedEntries);
        }
    }

    public void run() {
        MapService mapService = getService();
        if (data != null) {
            for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    Data key = recordReplicationInfo.getKey();
                    final Data value = recordReplicationInfo.getValue();
                    Record newRecord = mapService.createRecord(mapName, key, value, -1L, MapService.getNow());
                    mapService.applyRecordInfo(newRecord, recordReplicationInfo);
                    recordStore.putForReplication(key, newRecord);
                }
            }
        }
        if (mapInitialLoadInfo != null) {
            for (Entry<String, Boolean> entry : mapInitialLoadInfo.entrySet()) {
                final String mapName = entry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                recordStore.setLoaded(entry.getValue());
            }
        }
        for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), entry.getKey());
            final List<DelayedEntry> replicatedEntries = entry.getValue();
            final WriteBehindQueue<DelayedEntry> writeBehindQueue = recordStore.getWriteBehindQueue();
            writeBehindQueue.addEnd(replicatedEntries);
        }
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
        size = in.readInt();
        mapInitialLoadInfo = new HashMap<String, Boolean>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            boolean loaded = in.readBoolean();
            mapInitialLoadInfo.put(name, loaded);
        }
        size = in.readInt();
        delayedEntries = new HashMap<String, List<DelayedEntry>>(size);
        for (int i = 0; i < size; i++) {
            final String mapName = in.readUTF();
            final int listSize = in.readInt();
            final List<DelayedEntry> delayedEntriesList = new ArrayList<DelayedEntry>(listSize);
            for (int j = 0; j < listSize; j++) {
                final Data key = IOUtil.readNullableData(in);
                final Data value = IOUtil.readNullableData(in);
                final long storeTime = in.readLong();
                final int partitionId = in.readInt();
                final DelayedEntry<Data, Data> entry
                        = DelayedEntry.create(key, value, storeTime, partitionId);
                delayedEntriesList.add(entry);
            }
            delayedEntries.put(mapName, delayedEntriesList);
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
        out.writeInt(mapInitialLoadInfo.size());
        for (Entry<String, Boolean> entry : mapInitialLoadInfo.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeBoolean(entry.getValue());
        }
        final MapService mapService = getService();
        out.writeInt(delayedEntries.size());
        for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            out.writeUTF(entry.getKey());
            final List<DelayedEntry> delayedEntryList = entry.getValue();
            out.writeInt(delayedEntryList.size());
            for (DelayedEntry e : delayedEntryList) {
                final Data key = mapService.toData(e.getKey());
                final Data value = mapService.toData(e.getValue());
                IOUtil.writeNullableData(out, key);
                IOUtil.writeNullableData(out, value);
                out.writeLong(e.getStoreTime());
                out.writeInt(e.getPartitionId());
            }
        }
    }

    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }

}
