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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class MapReplicationOperation extends AbstractOperation implements MutatingOperation {

    private Map<String, Set<RecordReplicationInfo>> data;
    private Map<String, Collection<DelayedEntry>> delayedEntries;

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(MapService mapService, PartitionContainer container, int partitionId,
                                   int replicaIndex) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);

        data = new HashMap<String, Set<RecordReplicationInfo>>(container.getMaps().size());
        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            final MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            String name = entry.getKey();
            // now prepare data to migrate records
            Set<RecordReplicationInfo> recordSet = new HashSet<RecordReplicationInfo>(recordStore.size());
            final Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                Data key = record.getKey();
                RecordReplicationInfo recordReplicationInfo;
                recordReplicationInfo = createRecordReplicationInfo(key, record, mapService);
                recordSet.add(recordReplicationInfo);
            }
            data.put(name, recordSet);
        }
        readDelayedEntries(container);
    }

    private void readDelayedEntries(PartitionContainer container) {
        delayedEntries = new HashMap<String, Collection<DelayedEntry>>(container.getMaps().size());
        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            if (!mapContainer.getMapStoreContext().isWriteBehindMapStoreEnabled()) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> writeBehindQueue = ((WriteBehindStore) recordStore.getMapDataStore())
                    .getWriteBehindQueue();
            final Collection<DelayedEntry> delayedEntries = writeBehindQueue.asList();
            if (delayedEntries != null && delayedEntries.size() == 0) {
                continue;
            }
            this.delayedEntries.put(entry.getKey(), delayedEntries);
        }
    }

    @Override
    public void run() {
        MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (data != null) {
            for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), mapName);
                recordStore.reset();

                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    Data key = recordReplicationInfo.getKey();
                    final Data value = recordReplicationInfo.getValue();
                    Record newRecord = recordStore.createRecord(value, -1L, Clock.currentTimeMillis());
                    applyRecordInfo(newRecord, recordReplicationInfo);
                    recordStore.putRecord(key, newRecord);
                }
            }
        }

        for (Entry<String, Collection<DelayedEntry>> entry : delayedEntries.entrySet()) {
            RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), entry.getKey());
            WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
            mapDataStore.clear();

            Collection<DelayedEntry> replicatedEntries = entry.getValue();
            for (DelayedEntry delayedEntry : replicatedEntries) {
                mapDataStore.add(delayedEntry);
            }
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
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
        delayedEntries = new HashMap<String, Collection<DelayedEntry>>(size);
        for (int i = 0; i < size; i++) {
            final String mapName = in.readUTF();
            final int listSize = in.readInt();
            final List<DelayedEntry> delayedEntriesList = new ArrayList<DelayedEntry>(listSize);
            for (int j = 0; j < listSize; j++) {
                Data key = in.readData();
                Data value = in.readData();
                long storeTime = in.readLong();
                int partitionId = in.readInt();

                DelayedEntry<Data, Data> entry = DelayedEntries.createDefault(key, value, storeTime, partitionId);
                delayedEntriesList.add(entry);
            }
            delayedEntries.put(mapName, delayedEntriesList);
        }
    }

    @Override
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
        final MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        out.writeInt(delayedEntries.size());
        for (Entry<String, Collection<DelayedEntry>> entry : delayedEntries.entrySet()) {
            out.writeUTF(entry.getKey());
            final Collection<DelayedEntry> delayedEntryList = entry.getValue();
            out.writeInt(delayedEntryList.size());
            for (DelayedEntry e : delayedEntryList) {
                final Data key = mapServiceContext.toData(e.getKey());
                final Data value = mapServiceContext.toData(e.getValue());
                out.writeData(key);
                out.writeData(value);
                out.writeLong(e.getStoreTime());
                out.writeInt(e.getPartitionId());
            }
        }
    }

    public boolean isEmpty() {
        return (data == null || data.isEmpty())
                && (delayedEntries == null || delayedEntries.isEmpty());
    }

    private RecordReplicationInfo createRecordReplicationInfo(Data key, Record record, MapService mapService) {
        final RecordInfo info = Records.buildRecordInfo(record);
        return new RecordReplicationInfo(key, mapService.getMapServiceContext().toData(record.getValue()),
                info);
    }
}
