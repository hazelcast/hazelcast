/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.ExpirySystem;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.MapIndexInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * Holder for raw IMap key-value pairs and their metadata.
 */
// keep this `protected`, extended in another context.
public class MapReplicationStateHolder implements IdentifiedDataSerializable, Versioned {

    // holds recordStore-references of this partitions' maps
    protected transient Map<String, RecordStore<Record>> storesByMapName;

    // data for each map
    protected transient Map<String, List> data;

    // propagates the information if the given record store has been already loaded with map-loaded
    // if so, the loading won't be triggered again after a migration to avoid duplicate loading.
    protected transient Map<String, Boolean> loaded;

    // Definitions of indexes for each map. The indexes are sent in the map-replication operation for each partition
    // since only this approach guarantees that that there is no race between index migration and data migration.
    // Earlier the index definition used to arrive in the post-join operations, but these operation has no guarantee
    // on order of execution, so it was possible that the post-join operations were executed after some map-replication
    // operations, which meant that the index did not include some data.
    protected transient List<MapIndexInfo> mapIndexInfos;

    private MapReplicationOperation operation;

    /**
     * This constructor exists solely for instantiation by {@code MapDataSerializerHook}. The object is not ready to use
     * unless {@code operation} is set.
     */
    public MapReplicationStateHolder() {
    }

    public void setOperation(MapReplicationOperation operation) {
        this.operation = operation;
    }

    void prepare(PartitionContainer container, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        storesByMapName = createHashMap(namespaces.size());
        loaded = createHashMap(namespaces.size());
        mapIndexInfos = new ArrayList<>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace mapNamespace = (ObjectNamespace) namespace;
            String mapName = mapNamespace.getObjectName();
            RecordStore recordStore = container.getExistingRecordStore(mapName);
            if (recordStore == null) {
                continue;
            }

            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }

            loaded.put(mapName, recordStore.isLoaded());
            storesByMapName.put(mapName, recordStore);

            Set<IndexConfig> indexConfigs = new HashSet<>();
            if (mapContainer.isGlobalIndexEnabled()) {
                // global-index
                final Indexes indexes = mapContainer.getIndexes();
                for (Index index : indexes.getIndexes()) {
                    indexConfigs.add(index.getConfig());
                }
                indexConfigs.addAll(indexes.getIndexDefinitions());
            } else {
                // partitioned-index
                final Indexes indexes = mapContainer.getIndexes(container.getPartitionId());
                if (indexes != null && indexes.haveAtLeastOneIndexOrDefinition()) {
                    for (Index index : indexes.getIndexes()) {
                        indexConfigs.add(index.getConfig());
                    }
                    indexConfigs.addAll(indexes.getIndexDefinitions());
                }
            }
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapName);
            mapIndexInfo.addIndexCofigs(indexConfigs);
            mapIndexInfos.add(mapIndexInfo);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    void applyState() {
        ThreadUtil.assertRunningOnPartitionThread();

        applyIndexesState();

        if (!isNullOrEmpty(data)) {
            for (Map.Entry<String, List> dataEntry : data.entrySet()) {
                String mapName = dataEntry.getKey();
                List keyRecordExpiry = dataEntry.getValue();
                RecordStore recordStore = operation.getRecordStore(mapName);
                recordStore.reset();
                recordStore.setPreMigrationLoadedStatus(loaded.get(mapName));

                MapContainer mapContainer = recordStore.getMapContainer();
                PartitionContainer partitionContainer = recordStore.getMapContainer().getMapServiceContext()
                        .getPartitionContainer(operation.getPartitionId());
                for (Map.Entry<String, IndexConfig> indexDefinition : mapContainer.getIndexDefinitions().entrySet()) {
                    Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                    indexes.addOrGetIndex(indexDefinition.getValue());
                }

                final Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                final boolean populateIndexes = indexesMustBePopulated(indexes, operation);

                InternalIndex[] indexesSnapshot = null;

                if (populateIndexes) {
                    // defensively clear possible stale leftovers in non-global indexes from the previous failed promotion attempt
                    indexesSnapshot = indexes.getIndexes();

                    Indexes.beginPartitionUpdate(indexesSnapshot);

                    indexes.clearAll();
                }

                long nowInMillis = Clock.currentTimeMillis();

                for (int i = 0; i < keyRecordExpiry.size(); i += 3) {
                    Data dataKey = (Data) keyRecordExpiry.get(i);
                    Record record = (Record) keyRecordExpiry.get(i + 1);
                    ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.get(i + 2);
                    // TODO rolling upgrade
                    recordStore.putReplicatedRecord(dataKey, record, expiryMetadata, populateIndexes, nowInMillis);

                    if (recordStore.shouldEvict()) {
                        // No need to continue replicating records anymore.
                        // We are already over eviction threshold, each put record will cause another eviction.
                        recordStore.evictEntries(dataKey);
                        break;
                    }
                    recordStore.disposeDeferredBlocks();
                }

                if (populateIndexes) {
                    Indexes.markPartitionAsIndexed(partitionContainer.getPartitionId(), indexesSnapshot);
                }
            }
        }
    }

    private void applyIndexesState() {
        if (mapIndexInfos != null) {
            for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
                addIndexes(mapIndexInfo.getMapName(), mapIndexInfo.getIndexConfigs());
            }
        }
    }

    private void addIndexes(String mapName, Collection<IndexConfig> indexConfigs) {
        if (indexConfigs == null) {
            return;
        }
        RecordStore recordStore = operation.getRecordStore(mapName);
        MapContainer mapContainer = recordStore.getMapContainer();
        if (mapContainer.isGlobalIndexEnabled()) {
            // creating global indexes on partition thread in case they do not exist
            for (IndexConfig indexConfig : indexConfigs) {
                Indexes indexes = mapContainer.getIndexes();

                // optimisation not to synchronize each partition thread on the addOrGetIndex method
                if (indexes.getIndex(indexConfig.getName()) == null) {
                    indexes.addOrGetIndex(indexConfig);
                }
            }
        } else {
            Indexes indexes = mapContainer.getIndexes(operation.getPartitionId());
            indexes.createIndexesFromRecordedDefinitions();
            for (IndexConfig indexConfig : indexConfigs) {
                indexes.addOrGetIndex(indexConfig);
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(storesByMapName.size());

        for (Map.Entry<String, RecordStore<Record>> entry : storesByMapName.entrySet()) {
            String mapName = entry.getKey();
            out.writeUTF(mapName);

            SerializationService ss = getSerializationService(operation.getRecordStore(mapName).getMapContainer());
            RecordStore<Record> recordStore = entry.getValue();
            out.writeInt(recordStore.size());
            // No expiration should be done in forEach, since we have serialized size before.
            recordStore.forEach((dataKey, record) -> {
                try {
                    IOUtil.writeData(out, dataKey);
                    Records.writeRecord(out, record, ss.toData(record.getValue()),
                            recordStore.getExpirySystem().getExpiredMetadata(dataKey));
                } catch (IOException e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }, operation.getReplicaIndex() != 0, true);

//            if (out.getVersion().isGreaterOrEqual(Versions.V4_2)) {
//                ExpirySystem expirySystem = recordStore.getExpirySystem();
//                Map<Data, ExpiryMetadata> expireTimeByKey = expirySystem.getExpireTimeByKey();
//
//                out.writeInt(expireTimeByKey == null ? 0 : expireTimeByKey.size());
//                if (!MapUtil.isNullOrEmpty(expireTimeByKey)) {
//                    for (Map.Entry<Data, ExpiryMetadata> metadataEntry : expireTimeByKey.entrySet()) {
//                        Data key = metadataEntry.getKey();
//                        ExpiryMetadata expiryMetadata = metadataEntry.getValue();
//
//                        IOUtil.writeData(out, key);
//                        out.writeLong(expiryMetadata.getTtl());
//                        out.writeLong(expiryMetadata.getMaxIdle());
//                        out.writeLong(expiryMetadata.getExpirationTime());
//                    }
//                }
//            }
        }

        out.writeInt(loaded.size());
        for (Map.Entry<String, Boolean> loadedEntry : loaded.entrySet()) {
            out.writeUTF(loadedEntry.getKey());
            out.writeBoolean(loadedEntry.getValue());
        }

        out.writeInt(mapIndexInfos.size());
        for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
            out.writeObject(mapIndexInfo);
        }
    }

    private static SerializationService getSerializationService(MapContainer mapContainer) {
        return mapContainer.getMapServiceContext()
                .getNodeEngine().getSerializationService();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = createHashMap(size);

        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int numOfRecords = in.readInt();
            List keyRecordExpiry = new ArrayList<>(numOfRecords * 2);
            for (int j = 0; j < numOfRecords; j++) {
                Data dataKey = IOUtil.readData(in);
                ExpiryMetadata expiryMetadata = new ExpirySystem.ExpiryMetadataImpl();
                Record record = Records.readRecord(in, expiryMetadata);

                keyRecordExpiry.add(dataKey);
                keyRecordExpiry.add(record);
                keyRecordExpiry.add(expiryMetadata);
            }
            data.put(name, keyRecordExpiry);

//            if (in.getVersion().isGreaterOrEqual(Versions.V4_2)) {
//                int expirySize = in.readInt();
//                if (expirySize != 0) {
//                    Queue metadata = new ArrayDeque<>(expirySize);
//                    for (int j = 0; j < expirySize; j++) {
//                        Data key = IOUtil.readData(in);
//                        long ttl = in.readLong();
//                        long maxIdle = in.readLong();
//                        long expiryTime = in.readLong();
//
//                        metadata.add(key);
//                        metadata.add(ttl);
//                        metadata.add(maxIdle);
//                        metadata.add(expiryTime);
//                    }
//
//                    expiryMetaData.put(name, metadata);
//
//                }
//            }
        }

        int loadedSize = in.readInt();
        loaded = createHashMap(loadedSize);
        for (int i = 0; i < loadedSize; i++) {
            loaded.put(in.readUTF(), in.readBoolean());
        }

        int mapIndexInfoSize = in.readInt();
        mapIndexInfos = new ArrayList<>(mapIndexInfoSize);
        for (int i = 0; i < mapIndexInfoSize; i++) {
            MapIndexInfo mapIndexInfo = in.readObject();
            mapIndexInfos.add(mapIndexInfo);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_REPLICATION_STATE_HOLDER;
    }

    private static boolean indexesMustBePopulated(Indexes indexes, MapReplicationOperation operation) {
        if (!indexes.haveAtLeastOneIndex()) {
            // no indexes to populate
            return false;
        }

        if (indexes.isGlobal()) {
            // global indexes are populated during migration finalization
            return false;
        }

        if (operation.getReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return false;
        }

        return true;
    }
}
