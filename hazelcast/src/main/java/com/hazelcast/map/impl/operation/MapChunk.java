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
import com.hazelcast.internal.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class MapChunk extends Operation implements IdentifiedDataSerializable {

    private transient String mapName;
    private transient MapChunkContext context;
    private transient LinkedList keyRecordExpiry;
    private transient BooleanSupplier isEndOfChunk;

    private transient boolean loaded;
    private transient MapIndexInfo mapIndexInfo;
    private transient LocalRecordStoreStatsImpl stats;

    private boolean firstChunk;

    public MapChunk() {
    }

    public MapChunk(MapChunkContext context, BooleanSupplier isEndOfChunk, int chunkNumber) {
        this.context = context;
        this.isEndOfChunk = isEndOfChunk;
        this.firstChunk = (chunkNumber == 1);

        System.err.println("Chunk number ----> " + chunkNumber
                + ", mapName: " + context.getMapName()
                + ", partitionId: " + context.getPartitionId()
                + ", firstChunk: " + firstChunk);
    }

    @Override
    public void run() throws Exception {
        assert !keyRecordExpiry.isEmpty() : "no empty operation expected";

        boolean populateIndexes = false;
        InternalIndex[] indexesSnapshot = null;

        RecordStore recordStore = getRecordStore(mapName);
        if (firstChunk) {
            addIndexes(recordStore, mapIndexInfo.getIndexConfigs());
            recordStore.reset();
            recordStore.setStats(stats);
            recordStore.setPreMigrationLoadedStatus(loaded);

            MapContainer mapContainer = recordStore.getMapContainer();
            PartitionContainer partitionContainer = recordStore.getMapContainer().getMapServiceContext()
                    .getPartitionContainer(getPartitionId());
            for (Map.Entry<String, IndexConfig> indexDefinition : mapContainer.getIndexDefinitions().entrySet()) {
                Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                indexes.addOrGetIndex(indexDefinition.getValue());
            }

            final Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
            populateIndexes = indexesMustBePopulated(indexes);

            if (populateIndexes) {
                // defensively clear possible stale
                // leftovers in non-global indexes from
                // the previous failed promotion attempt
                indexesSnapshot = indexes.getIndexes();
                Indexes.beginPartitionUpdate(indexesSnapshot);
                indexes.clearAll();
            }
        }

        long nowInMillis = Clock.currentTimeMillis();
        do {
            Data dataKey = (Data) keyRecordExpiry.poll();
            Record record = (Record) keyRecordExpiry.poll();
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.poll();

            // TODO add indexesMustBePopulated check into IndexingObserver
            recordStore.putOrUpdateReplicatedRecord(dataKey, record, expiryMetadata,
                    getReplicaIndex() == 0, nowInMillis);

        } while (!keyRecordExpiry.isEmpty());

        // TODO check if this is problematic or we need a flag to indicate end of chunks
        if (firstChunk) {
            if (populateIndexes) {
                Indexes.markPartitionAsIndexed(getPartitionId(), indexesSnapshot);
            }
        }
    }

    private void addIndexes(RecordStore recordStore, Collection<IndexConfig> indexConfigs) {
        if (indexConfigs == null) {
            return;
        }

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
            Indexes indexes = mapContainer.getIndexes(getPartitionId());
            indexes.createIndexesFromRecordedDefinitions();
            for (IndexConfig indexConfig : indexConfigs) {
                indexes.addOrGetIndex(indexConfig);
            }
        }
    }

    private boolean indexesMustBePopulated(Indexes indexes) {
        if (!indexes.haveAtLeastOneIndex()) {
            // no indexes to populate
            return false;
        }

        if (indexes.isGlobal()) {
            // global indexes are populated during migration finalization
            return false;
        }

        if (getReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return false;
        }

        return true;
    }

    private RecordStore getRecordStore(String mapName) {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), mapName, true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeBoolean(firstChunk);
        if (firstChunk) {
            MapIndexInfo mapIndexInfo = context.createMapIndexInfo();
            out.writeObject(mapIndexInfo);
            out.writeBoolean(context.isRecordStoreLoaded());
            context.getStats().writeData(out);

            firstChunk = false;
        }

        writeChunk((BufferObjectDataOutput) out, context);
    }

    private void writeChunk(BufferObjectDataOutput out, MapChunkContext context) throws IOException {
        SerializationService ss = context.getSerializationService();

        out.writeString(context.getMapName());
        Iterator<Map.Entry<Data, Record>> entries = context.getIterator();
        while (entries.hasNext()) {
            Map.Entry<Data, Record> entry = entries.next();

            Data dataKey = entry.getKey();
            Record record = entry.getValue();
            Data dataValue = ss.toData(record.getValue());

            IOUtil.writeData(out, dataKey);
            Records.writeRecord(out, record, dataValue);
            Records.writeExpiry(out, context.getExpiryMetadata(dataKey));

            if (isEndOfChunk.getAsBoolean()) {
                break;
            }
        }

        // indicates end of chunk
        IOUtil.writeData(out, null);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        this.firstChunk = in.readBoolean();
        if (firstChunk) {
            this.mapIndexInfo = in.readObject();
            this.loaded = in.readBoolean();
            this.stats = new LocalRecordStoreStatsImpl();
            stats.readData(in);
        }

        readChunk(in);
    }

    private void readChunk(ObjectDataInput in) throws IOException {
        this.mapName = in.readString();
        LinkedList keyRecordExpiry = new LinkedList<>();
        do {
            Data dataKey = IOUtil.readData(in);
            // null indicates end of chunk
            if (dataKey == null) {
                break;
            }

            Record record = Records.readRecord(in);
            ExpiryMetadata expiryMetadata = Records.readExpiry(in);

            keyRecordExpiry.add(dataKey);
            keyRecordExpiry.add(record);
            keyRecordExpiry.add(expiryMetadata);

        } while (true);

        this.keyRecordExpiry = keyRecordExpiry;

        System.err.println("Read chunk: " + keyRecordExpiry.size() / 3);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_CHUNK;
    }

}
