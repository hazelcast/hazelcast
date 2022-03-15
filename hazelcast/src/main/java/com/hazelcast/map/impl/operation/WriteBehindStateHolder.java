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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries.newAddedDelayedEntry;

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
    private Map<String, Map<UUID, Long>> reservationsByTxnIdPerMap;

    /**
     * This constructor exists solely for instantiation by
     * {@code MapDataSerializerHook}. The object is not ready
     * to use unless {@code mapReplicationOperation} is set.
     */
    public WriteBehindStateHolder() {
    }

    public void setMapReplicationOperation(MapReplicationOperation mapReplicationOperation) {
        this.mapReplicationOperation = mapReplicationOperation;
    }

    void prepare(PartitionContainer container, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        int size = namespaces.size();

        flushSequences = createHashMap(size);
        delayedEntries = createHashMap(size);
        reservationsByTxnIdPerMap = createHashMap(size);

        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace mapNamespace = (ObjectNamespace) namespace;
            String mapName = mapNamespace.getObjectName();
            RecordStore recordStore = container.getRecordStore(mapName);
            if (recordStore == null) {
                continue;
            }

            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex
                    || !mapContainer.getMapStoreContext().isWriteBehindMapStoreEnabled()) {
                continue;
            }

            WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
            reservationsByTxnIdPerMap.put(mapName,
                    mapDataStore.getTxnReservedCapacityCounter().getReservedCapacityCountPerTxnId());

            WriteBehindQueue<DelayedEntry> writeBehindQueue = mapDataStore.getWriteBehindQueue();
            List<DelayedEntry> entries = writeBehindQueue.asList();
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            delayedEntries.put(mapName, entries);
            flushSequences.put(mapName, new ArrayDeque<>(mapDataStore.getFlushSequences()));
        }

    }

    void applyState() {
        for (Map.Entry<String, Map<UUID, Long>> entry : reservationsByTxnIdPerMap.entrySet()) {
            String mapName = entry.getKey();
            Map<UUID, Long> reservationsByTxnId = entry.getValue();
            RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
            WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
            mapDataStore.getTxnReservedCapacityCounter().putAll(reservationsByTxnId);
        }

        for (Map.Entry<String, List<DelayedEntry>> entry : delayedEntries.entrySet()) {
            String mapName = entry.getKey();
            RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
            WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();

            mapDataStore.reset();
            mapDataStore.setFlushSequences(flushSequences.get(mapName));

            Collection<DelayedEntry> replicatedEntries = entry.getValue();
            for (DelayedEntry delayedEntry : replicatedEntries) {
                mapDataStore.addForcibly(delayedEntry);
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
            out.writeString(entry.getKey());
            List<DelayedEntry> delayedEntryList = entry.getValue();
            out.writeInt(delayedEntryList.size());

            for (DelayedEntry e : delayedEntryList) {
                Data key = mapServiceContext.toData(e.getKey());
                Data value = mapServiceContext.toData(e.getValue());
                long expirationTime = e.getExpirationTime();

                IOUtil.writeData(out, key);
                IOUtil.writeData(out, value);
                out.writeLong(expirationTime);
                out.writeLong(e.getStoreTime());
                out.writeInt(e.getPartitionId());
                out.writeLong(e.getSequence());
                UUIDSerializationUtil.writeUUID(out, e.getTxnId());
            }
        }

        out.writeInt(flushSequences.size());
        for (Map.Entry<String, Queue<WriteBehindStore.Sequence>> entry : flushSequences.entrySet()) {
            out.writeString(entry.getKey());
            Queue<WriteBehindStore.Sequence> queue = entry.getValue();
            out.writeInt(queue.size());
            for (WriteBehindStore.Sequence sequence : queue) {
                out.writeLong(sequence.getSequence());
                out.writeBoolean(sequence.isFullFlush());
            }
        }

        out.writeInt(reservationsByTxnIdPerMap.size());
        for (Map.Entry<String, Map<UUID, Long>> entry : reservationsByTxnIdPerMap.entrySet()) {
            out.writeString(entry.getKey());
            Map<UUID, Long> reservationsByTxnId = entry.getValue();
            out.writeInt(reservationsByTxnId.size());
            for (Map.Entry<UUID, Long> counterByTxnId : reservationsByTxnId.entrySet()) {
                writeUUID(out, counterByTxnId.getKey());
                out.writeLong(counterByTxnId.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        delayedEntries = createHashMap(size);

        for (int i = 0; i < size; i++) {
            String mapName = in.readString();
            int listSize = in.readInt();
            List<DelayedEntry> delayedEntriesList = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; j++) {
                Data key = IOUtil.readData(in);
                Data value = IOUtil.readData(in);
                long expirationTime = in.readLong();
                long storeTime = in.readLong();
                int partitionId = in.readInt();
                long sequence = in.readLong();
                UUID txnId = UUIDSerializationUtil.readUUID(in);

                DelayedEntry<Data, Data> entry
                        = newAddedDelayedEntry(key, value, expirationTime, storeTime, partitionId, txnId);
                entry.setSequence(sequence);
                delayedEntriesList.add(entry);
            }

            delayedEntries.put(mapName, delayedEntriesList);
        }

        int expectedSize = in.readInt();
        flushSequences = createHashMap(expectedSize);
        for (int i = 0; i < expectedSize; i++) {
            String mapName = in.readString();
            int setSize = in.readInt();
            Queue<WriteBehindStore.Sequence> queue = new ArrayDeque<>(setSize);
            for (int j = 0; j < setSize; j++) {
                queue.add(new WriteBehindStore.Sequence(in.readLong(), in.readBoolean()));
            }
            flushSequences.put(mapName, queue);
        }

        int mapCount = in.readInt();
        reservationsByTxnIdPerMap = mapCount == 0 ? Collections.emptyMap() : new HashMap<>(mapCount);
        for (int i = 0; i < mapCount; i++) {
            String mapName = in.readString();
            int numOfCounters = in.readInt();
            Map<UUID, Long> counterByTxnId = createHashMap(numOfCounters);
            for (int j = 0; j < numOfCounters; j++) {
                counterByTxnId.put(readUUID(in), in.readLong());
            }
            reservationsByTxnIdPerMap.put(mapName, counterByTxnId);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.WRITE_BEHIND_STATE_HOLDER;
    }
}
