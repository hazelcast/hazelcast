/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Holder for write-behind-specific state.
 */
public class WriteBehindStateHolder implements IdentifiedDataSerializable {

    private MapReplicationOperation mapReplicationOperation;
    private Map<String, List<DelayedEntry>> delayedEntries;

    /**
     * @see WriteBehindStore#flushSequences
     */
    private Map<String, Queue<WriteBehindStore.Sequence>> flushSequences;

    /**
     * This constructor exists solely for instantiation by {@code MapDataSerializerHook}. The object is not ready to use
     * unless {@code mapReplicationOperation} is set.
     */
    public WriteBehindStateHolder() {
    }

    public WriteBehindStateHolder(MapReplicationOperation mapReplicationOperation) {
        this.mapReplicationOperation = mapReplicationOperation;
    }

    void prepare(PartitionContainer container, int replicaIndex) {
        int size = container.getMaps().size();

        flushSequences = new HashMap<String, Queue<WriteBehindStore.Sequence>>(size);
        delayedEntries = new HashMap<String, List<DelayedEntry>>(size);

        for (Map.Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
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


    void applyState() {
        for (Map.Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            String mapName = entry.getKey();
            RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
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
        MapService mapService = mapReplicationOperation.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        out.writeInt(delayedEntries.size());
        for (Map.Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
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
        for (Map.Entry<String, Queue<WriteBehindStore.Sequence>> entry : flushSequences.entrySet()) {
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

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.WRITE_BEHIND_STATE_HOLDER;
    }
}
