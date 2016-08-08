/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 */
public class MapReplicationOperation extends Operation implements MutatingOperation {

    // keep these fields `protected`, extended in another context.
    protected final MapReplicationStateHolder mapReplicationStateHolder = new MapReplicationStateHolder();
    protected final WriteBehindStateHolder writeBehindStateHolder = new WriteBehindStateHolder();

    public MapReplicationOperation() {
    }

    public MapReplicationOperation(PartitionContainer container, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);

        mapReplicationStateHolder.prepare(container, replicaIndex);
        writeBehindStateHolder.prepare(container, replicaIndex);
    }


    @Override
    public void run() {
        mapReplicationStateHolder.applyState();
        writeBehindStateHolder.applyState();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        mapReplicationStateHolder.readData(in);
        writeBehindStateHolder.readData(in);
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        mapReplicationStateHolder.writeData(out);
        writeBehindStateHolder.writeData(out);
    }

    private RecordReplicationInfo createRecordReplicationInfo(Data key, Record record, MapServiceContext mapServiceContext) {
        RecordInfo info = buildRecordInfo(record);
        return new RecordReplicationInfo(key, mapServiceContext.toData(record.getValue()), info);
    }

    private RecordStore getRecordStore(String mapName) {
        final boolean skipLoadingOnRecordStoreCreate = true;
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), mapName, skipLoadingOnRecordStoreCreate);
    }

    /**
     * Holder for raw IMap key-value pairs and their metadata.
     */
    // keep this `protected`, extended in another context.
    protected class MapReplicationStateHolder implements DataSerializable {

        protected Map<String, Set<RecordReplicationInfo>> data;
        // propagates the information if the given record store has been already loaded with map-loaded
        // if so, the loading won't be triggered again after a migration to avoid duplicate loading.
        protected Map<String, Boolean> loaded;

        private void prepare(PartitionContainer container, int replicaIndex) {
            data = new HashMap<String, Set<RecordReplicationInfo>>(container.getMaps().size());
            loaded = new HashMap<String, Boolean>(container.getMaps().size());
            for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
                RecordStore recordStore = entry.getValue();

                MapContainer mapContainer = recordStore.getMapContainer();
                MapConfig mapConfig = mapContainer.getMapConfig();
                if (mapConfig.getTotalBackupCount() < replicaIndex) {
                    continue;
                }
                MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
                String mapName = entry.getKey();
                loaded.put(mapName, recordStore.isLoaded());
                // now prepare data to migrate records
                Set<RecordReplicationInfo> recordSet = new HashSet<RecordReplicationInfo>(recordStore.size());
                final Iterator<Record> iterator = recordStore.iterator();
                while (iterator.hasNext()) {
                    Record record = iterator.next();
                    Data key = record.getKey();
                    RecordReplicationInfo recordReplicationInfo
                            = createRecordReplicationInfo(key, record, mapServiceContext);
                    recordSet.add(recordReplicationInfo);
                }
                data.put(mapName, recordSet);
            }
        }

        private void applyState() {
            if (data != null) {
                for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                    Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                    final String mapName = dataEntry.getKey();
                    RecordStore recordStore = getRecordStore(mapName);
                    recordStore.reset();
                    recordStore.setPreMigrationLoadedStatus(loaded.get(mapName));

                    for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                        Data key = recordReplicationInfo.getKey();
                        final Data value = recordReplicationInfo.getValue();
                        Record newRecord = recordStore.createRecord(value, -1L, Clock.currentTimeMillis());
                        applyRecordInfo(newRecord, recordReplicationInfo);
                        recordStore.putRecord(key, newRecord);
                    }
                }
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(data.size());
            for (Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                out.writeUTF(dataEntry.getKey());
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                out.writeInt(recordReplicationInfos.size());
                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    out.writeObject(recordReplicationInfo);
                }
            }

            out.writeInt(loaded.size());
            for (Entry<String, Boolean> loadedEntry : loaded.entrySet()) {
                out.writeUTF(loadedEntry.getKey());
                out.writeBoolean(loadedEntry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
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

            int loadedSize = in.readInt();
            loaded = new HashMap<String, Boolean>(loadedSize);
            for (int i = 0; i < loadedSize; i++) {
                loaded.put(in.readUTF(), in.readBoolean());
            }
        }
    }


    /**
     * Holder for write-behind-specific state.
     */
    private class WriteBehindStateHolder implements DataSerializable {

        private Map<String, List<DelayedEntry>> delayedEntries;

        /**
         * @see WriteBehindStore#flushSequences
         */
        private Map<String, Queue<WriteBehindStore.Sequence>> flushSequences;


        private void prepare(PartitionContainer container, int replicaIndex) {
            int size = container.getMaps().size();

            flushSequences = new HashMap<String, Queue<WriteBehindStore.Sequence>>(size);
            delayedEntries = new HashMap<String, List<DelayedEntry>>(size);

            for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
                RecordStore recordStore = entry.getValue();
                MapContainer mapContainer = recordStore.getMapContainer();
                MapConfig mapConfig = mapContainer.getMapConfig();
                if (mapConfig.getTotalBackupCount() < replicaIndex
                        || !mapContainer.getMapStoreContext().isWriteBehindMapStoreEnabled()) {
                    continue;
                }

                WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
                WriteBehindQueue<DelayedEntry> writeBehindQueue = mapDataStore.getWriteBehindQueue();
                List<DelayedEntry> entries = writeBehindQueue.asList();
                if (entries == null || entries.isEmpty()) {
                    continue;
                }

                String mapName = entry.getKey();

                delayedEntries.put(mapName, entries);
                flushSequences.put(mapName, new ArrayDeque<WriteBehindStore.Sequence>(mapDataStore.getFlushSequences()));
            }
        }


        private void applyState() {
            for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
                String mapName = entry.getKey();
                RecordStore recordStore = getRecordStore(mapName);
                WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();

                mapDataStore.reset();
                mapDataStore.setFlushSequences(flushSequences.get(mapName));

                Collection<DelayedEntry> replicatedEntries = entry.getValue();
                for (DelayedEntry delayedEntry : replicatedEntries) {
                    mapDataStore.add(delayedEntry);
                    mapDataStore.setSequence(delayedEntry.getSequence());
                }
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            MapService mapService = getService();
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();

            out.writeInt(delayedEntries.size());
            for (Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
                out.writeUTF(entry.getKey());
                List<DelayedEntry> delayedEntryList = entry.getValue();
                out.writeInt(delayedEntryList.size());

                for (DelayedEntry e : delayedEntryList) {
                    Data key = mapServiceContext.toData(e.getKey());
                    Data value = mapServiceContext.toData(e.getValue());

                    out.writeData(key);
                    out.writeData(value);
                    out.writeLong(e.getStoreTime());
                    out.writeInt(e.getPartitionId());
                    out.writeLong(e.getSequence());
                }
            }

            out.writeInt(flushSequences.size());
            for (Entry<String, Queue<WriteBehindStore.Sequence>> entry : flushSequences.entrySet()) {
                out.writeUTF(entry.getKey());
                Queue<WriteBehindStore.Sequence> queue = entry.getValue();
                out.writeInt(queue.size());
                for (WriteBehindStore.Sequence sequence : queue) {
                    out.writeLong(sequence.getSequence());
                    out.writeBoolean(sequence.isFullFlush());
                }
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();

            delayedEntries = new HashMap<String, List<DelayedEntry>>(size);

            for (int i = 0; i < size; i++) {
                String mapName = in.readUTF();
                int listSize = in.readInt();
                List<DelayedEntry> delayedEntriesList = new ArrayList<DelayedEntry>(listSize);
                for (int j = 0; j < listSize; j++) {
                    Data key = in.readData();
                    Data value = in.readData();
                    long storeTime = in.readLong();
                    int partitionId = in.readInt();
                    long sequence = in.readLong();

                    DelayedEntry<Data, Data> entry = DelayedEntries.createDefault(key, value, storeTime, partitionId);
                    entry.setSequence(sequence);
                    delayedEntriesList.add(entry);
                }

                delayedEntries.put(mapName, delayedEntriesList);
            }

            int expectedSize = in.readInt();
            flushSequences = new HashMap<String, Queue<WriteBehindStore.Sequence>>(expectedSize);
            for (int i = 0; i < expectedSize; i++) {
                String mapName = in.readUTF();
                int setSize = in.readInt();
                Queue<WriteBehindStore.Sequence> queue = new ArrayDeque<WriteBehindStore.Sequence>(setSize);
                for (int j = 0; j < setSize; j++) {
                    queue.add(new WriteBehindStore.Sequence(in.readLong(), in.readBoolean()));
                }
                flushSequences.put(mapName, queue);
            }

        }
    }
}
