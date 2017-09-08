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
import com.hazelcast.internal.cluster.Versions;
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
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ThreadUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

/**
 * Holder for raw IMap key-value pairs and their metadata.
 */
// keep this `protected`, extended in another context.
public class MapReplicationStateHolder implements IdentifiedDataSerializable, Versioned {

    // data for each map
    protected Map<String, Set<RecordReplicationInfo>> data;

    // propagates the information if the given record store has been already loaded with map-loaded
    // if so, the loading won't be triggered again after a migration to avoid duplicate loading.
    protected Map<String, Boolean> loaded;

    // Definitions of indexes for each map. The indexes are sent in the map-replication operation for each partition
    // since only this approach guarantees that that there is no race between index migration and data migration.
    // Earlier the index definition used to arrive in the post-join operations, but these operation has no guarantee
    // on order of execution, so it was possible that the post-join operations were executed after some map-replication
    // operations, which meant that the index did not include some data.
    protected List<MapIndexInfo> mapIndexInfos;

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
        mapIndexInfos = new ArrayList<MapIndexInfo>(namespaces.size());
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

            Set<IndexInfo> indexInfos = new HashSet<IndexInfo>();
            if (mapContainer.isGlobalIndexEnabled()) {
                // global-index
                for (Index index : mapContainer.getIndexes().getIndexes()) {
                    indexInfos.add(new IndexInfo(index.getAttributeName(), index.isOrdered()));
                }
            } else {
                // partitioned-index
                final Indexes indexes = mapContainer.getIndexes(container.getPartitionId());
                if (indexes != null && indexes.hasIndex()) {
                    for (Index index : indexes.getIndexes()) {
                        indexInfos.add(new IndexInfo(index.getAttributeName(), index.isOrdered()));
                    }
                }
            }
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapName);
            mapIndexInfo.addIndexInfos(indexInfos);
            mapIndexInfos.add(mapIndexInfo);
        }

    }

    void applyState() {
        ThreadUtil.assertRunningOnPartitionThread();

        // the null check can be removed in 3.10+ codebase
        if (mapIndexInfos != null) {
            for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
                addIndexes(mapIndexInfo.getMapName(), mapIndexInfo.getIndexInfos());
            }
        }

        // RU_COMPAT_38
        // Old nodes (3.8-) won't send mapIndexInfos to new nodes (3.9+) in the map-replication operation.
        // This is the reason why we pick up the mapContainer.getIndexesToAdd() that were added by the PostJoinMapOperation
        // and we add them to the map, before we add data
        for (String mapName : data.keySet()) {
            RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
            MapContainer mapContainer = recordStore.getMapContainer();
            addIndexes(mapName, mapContainer.getPartitionIndexesToAdd());
        }

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

    private void addIndexes(String mapName, Collection<IndexInfo> indexInfos) {
        if (indexInfos == null) {
            return;
        }
        RecordStore recordStore = mapReplicationOperation.getRecordStore(mapName);
        MapContainer mapContainer = recordStore.getMapContainer();
        if (mapContainer.isGlobalIndexEnabled()) {
            // creating global indexes on partition thread in case they do not exist
            for (IndexInfo indexInfo : indexInfos) {
                Indexes indexes = mapContainer.getIndexes();
                // optimisation not to synchronize each partition thread on the addOrGetIndex method
                if (indexes.getIndex(indexInfo.getAttributeName()) == null) {
                    indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
                }
            }
        } else {
            Indexes indexes = mapContainer.getIndexes(mapReplicationOperation.getPartitionId());
            for (IndexInfo indexInfo : indexInfos) {
                indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
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

        // RU_COMPAT_39, the check can be removed in 3.9+ (the data should be then send unconditionally)
        // This information is carried over only for 3.9+ cluster versions
        if (out.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            out.writeInt(mapIndexInfos.size());
            for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
                out.writeObject(mapIndexInfo);
            }
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

        // RU_COMPAT_39, the check can be removed in 3.9+ (the data should be then read unconditionally)
        // This information is carried over only for 3.9+ cluster versions
        if (in.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            int mapIndexInfosSize = in.readInt();
            mapIndexInfos = new ArrayList<MapIndexInfo>(mapIndexInfosSize);
            for (int i = 0; i < mapIndexInfosSize; i++) {
                MapIndexInfo mapIndexInfo = in.readObject();
                mapIndexInfos.add(mapIndexInfo);
            }
        } else {
            // setting to null means we operate in 3.8- compatibility mode
            mapIndexInfos = null;
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
