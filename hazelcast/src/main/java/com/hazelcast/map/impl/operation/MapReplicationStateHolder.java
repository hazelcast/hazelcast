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
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

/**
 * Holder for raw IMap key-value pairs and their metadata.
 */
// keep this `protected`, extended in another context.
public class MapReplicationStateHolder implements IdentifiedDataSerializable, Versioned {

    protected Map<String, Set<RecordReplicationInfo>> data;
    // propagates the information if the given record store has been already loaded with map-loaded
    // if so, the loading won't be triggered again after a migration to avoid duplicate loading.
    protected Map<String, Boolean> loaded;

    private MapReplicationOperation mapReplicationOperation;

    /**
     * This constructor exists solely for instantiation by {@code MapDataSerializerHook}. The object is not ready to use
     * unless {@code mapReplicationOperation} is set.
     */
    public MapReplicationStateHolder() {
    }

    public MapReplicationStateHolder(MapReplicationOperation mapReplicationOperation) {
        this.mapReplicationOperation = mapReplicationOperation;
    }

    void prepare(PartitionContainer container, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        data = new HashMap<String, Set<RecordReplicationInfo>>(namespaces.size());
        loaded = new HashMap<String, Boolean>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace mapNamespace = (ObjectNamespace) namespace;
            String mapName = mapNamespace.getObjectName();
            RecordStore recordStore = container.getRecordStore(mapName);
            if (recordStore == null) {
                continue;
            }

            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }

            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            loaded.put(mapName, recordStore.isLoaded());
            // now prepare data to migrate records
            Set<RecordReplicationInfo> recordSet = new HashSet<RecordReplicationInfo>(recordStore.size());
            final Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                Data key = record.getKey();
                RecordReplicationInfo recordReplicationInfo
                        = mapReplicationOperation.createRecordReplicationInfo(key, record, mapServiceContext);
                recordSet.add(recordReplicationInfo);
            }
            data.put(mapName, recordSet);
        }
    }

    void applyState() {
        if (data != null) {
            for (Map.Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
                recordStore.reset();
                recordStore.setPreMigrationLoadedStatus(loaded.get(mapName));

                MapContainer mapContainer = recordStore.getMapContainer();
                PartitionContainer partitionContainer = recordStore.getMapContainer().getMapServiceContext()
                        .getPartitionContainer(mapReplicationOperation.getPartitionId());
                for (Map.Entry<String, Boolean> indexDefinition : mapContainer.getIndexDefinitions().entrySet()) {
                    Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                    indexes.addOrGetIndex(indexDefinition.getKey(), indexDefinition.getValue());
                }

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
        for (Map.Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
            out.writeUTF(dataEntry.getKey());
            Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
            out.writeInt(recordReplicationInfos.size());
            for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                out.writeObject(recordReplicationInfo);
            }
        }

        out.writeInt(loaded.size());
        for (Map.Entry<String, Boolean> loadedEntry : loaded.entrySet()) {
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

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_REPLICATION_STATE_HOLDER;
    }
}
