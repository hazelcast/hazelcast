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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.writebehind.DelayedEntry;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

/**
 * @author mdogan 7/24/12
 */
public class MapReplicationOperation extends AbstractOperation {

    private Map<String, Set<RecordReplicationInfo>> data;
    private Map<String, List<DelayedEntry>> delayedEntries;

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(MapService mapService, PartitionContainer container, int partitionId, int replicaIndex) {
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
                final Record record = iterator.next();
                RecordReplicationInfo recordReplicationInfo;
                recordReplicationInfo = createRecordReplicationInfo(record, mapService);
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
            MapContainer mapContainer = recordStore.getMapContainer();
            if (!mapContainer.isWriteBehindMapStoreEnabled()) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> writeBehindQueue
                    = ((WriteBehindStore) recordStore.getMapDataStore()).getWriteBehindQueue();
            final List<DelayedEntry> delayedEntries = writeBehindQueue.getSnapShot().asList();
            if (delayedEntries != null && delayedEntries.size() == 0) {
                continue;
            }
            this.delayedEntries.put(entry.getKey(), delayedEntries);
        }
    }

    public void run() {
        MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (data != null) {
            for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), mapName);
                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    Data key = recordReplicationInfo.getKey();
                    final Data value = recordReplicationInfo.getValue();
                    final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
                    Record newRecord = mapContainer.createRecord(key, value, -1L, Clock.currentTimeMillis());
                    applyRecordInfo(newRecord, recordReplicationInfo);
                    recordStore.putRecord(key, newRecord);
                }
                recordStore.setLoaded(true);

            }
        }
        for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            final RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), entry.getKey());
            final List<DelayedEntry> replicatedEntries = entry.getValue();
            final WriteBehindQueue<DelayedEntry> writeBehindQueue
                    = ((WriteBehindStore) recordStore.getMapDataStore()).getWriteBehindQueue();
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
        final MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        out.writeInt(delayedEntries.size());
        for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            out.writeUTF(entry.getKey());
            final List<DelayedEntry> delayedEntryList = entry.getValue();
            out.writeInt(delayedEntryList.size());
            for (DelayedEntry e : delayedEntryList) {
                final Data key = mapServiceContext.toData(e.getKey());
                final Data value = mapServiceContext.toData(e.getValue());
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

    private RecordReplicationInfo createRecordReplicationInfo(Record record, MapService mapService) {
        final RecordInfo info = Records.buildRecordInfo(record);
        return new RecordReplicationInfo(record.getKey(), mapService.getMapServiceContext().toData(record.getValue()), info);
    }

}
