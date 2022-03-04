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

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryReason;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries.newAddedDelayedEntry;

/**
 * Represents a chunk of migrated data per map.
 * <p>
 *
 * @see #writeChunk
 */
@SuppressWarnings("checkstyle:MethodCount")
public class MapChunk extends Operation implements IdentifiedDataSerializable {

    private static final int DISPOSE_AT_COUNT = 1024;

    protected transient ILogger logger;
    protected transient MapChunkContext context;
    protected transient BooleanSupplier isEndOfChunk;
    protected transient String mapName;

    private transient boolean loaded;
    private transient long currentSequence;
    private transient boolean hasWriteBehindState;
    private transient Queue sequences;
    private transient Map counterByTxnId;
    private transient UUID partitionUuid;
    private transient MapIndexInfo mapIndexInfo;
    private transient LinkedList keyRecordExpiry;
    private transient LocalRecordStoreStatsImpl stats;
    private transient List<DelayedEntry> delayedEntriesList;

    private boolean firstChunk;
    private boolean lastChunk;

    public MapChunk() {
    }

    public MapChunk(MapChunkContext context, int chunkNumber,
                    BooleanSupplier isEndOfChunk) {
        this.context = context;
        this.isEndOfChunk = isEndOfChunk;
        this.firstChunk = (chunkNumber == 1);
        this.logger = context.getLogger(getClass().getName());

        if (logger.isFinestEnabled()) {
            logger.finest(String.format("mapName:%s, chunkNumber:%d, partitionId:%d",
                    context.getMapName(), chunkNumber, context.getPartitionId()));
        }
    }

    @Override
    public final void run() throws Exception {
        RecordStore recordStore = getRecordStore(mapName);

        if (firstChunk) {
            addIndexes(recordStore, mapIndexInfo.getIndexConfigs());
            initializeRecordStore(mapName, recordStore);
            recordStore.setStats(stats);
            recordStore.setPreMigrationLoadedStatus(loaded);

            applyWriteBehindState(recordStore);
            applyNearCacheState(recordStore);

            applyIndexStateBefore(recordStore);
        }

        if (isNotEmpty(keyRecordExpiry)) {
            putInto(recordStore);
            logProgress(recordStore);
        }

        if (lastChunk) {
            applyIndexStateAfter(recordStore);
        }
    }

    @Override
    public final void beforeRun() {
        RecordStore recordStore = getRecordStore(mapName);
        recordStore.beforeOperation();
    }

    @Override
    public final void afterRunFinal() {
        RecordStore recordStore = getRecordStore(mapName);
        recordStore.afterOperation();
    }

    protected void incrementReplicationCount() {
        context.getMapStats().getReplicationStats().incrementFullPartitionReplicationCount();
    }

    protected void incrementReplicationRecordCount(long delta) {
        context.getMapStats().getReplicationStats().incrementFullPartitionReplicationRecordsCount(delta);
    }

    protected void initializeRecordStore(String mapName, RecordStore recordStore) {
        recordStore.reset();
    }

    private void putInto(RecordStore recordStore) {
        if (hasPerNodeEviction(recordStore)) {
            putOrUpdateReplicatedDataWithPerNodeEviction(recordStore);
        } else {
            putOrUpdateReplicatedData(recordStore);
        }
    }

    private void logProgress(RecordStore recordStore) {
        ILogger logger = recordStore.getMapContainer().getMapServiceContext()
                .getNodeEngine().getLogger(getClass().getName());
        if (logger.isFinestEnabled()) {
            logger.finest(String.format("mapName:%s, partitionId:%d,"
                            + " numberOfEntriesMigrated:%d", mapName,
                    getPartitionId(), (keyRecordExpiry.size() / 3)));
        }
    }

    private void applyIndexStateAfter(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        Indexes indexes = mapContainer.getIndexes(recordStore.getPartitionId());

        if (!indexesMustBePopulated(indexes)) {
            return;
        }

        Indexes.markPartitionAsIndexed(getPartitionId(), indexes.getIndexes());
    }

    private void applyIndexStateBefore(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        PartitionContainer partitionContainer = mapContainer.getMapServiceContext()
                .getPartitionContainer(getPartitionId());

        for (Map.Entry<String, IndexConfig> indexDefinition : mapContainer.getIndexDefinitions().entrySet()) {
            Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
            indexes.addOrGetIndex(indexDefinition.getValue());
        }

        Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
        boolean populateIndexes = indexesMustBePopulated(indexes);

        if (populateIndexes) {
            // defensively clear possible stale
            // leftovers in non-global indexes from
            // the previous failed promotion attempt
            Indexes.beginPartitionUpdate(indexes.getIndexes());
            indexes.clearAll();
        }
    }

    private void putOrUpdateReplicatedData(RecordStore recordStore) {
        long nowInMillis = Clock.currentTimeMillis();
        int count = 0;
        do {
            Data dataKey = (Data) keyRecordExpiry.poll();
            Record record = (Record) keyRecordExpiry.poll();
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.poll();

            Indexes indexes = recordStore.getMapContainer().getIndexes(recordStore.getPartitionId());

            recordStore.putOrUpdateReplicatedRecord(dataKey, record, expiryMetadata,
                    indexesMustBePopulated(indexes), nowInMillis);

            if (recordStore.shouldEvict()) {
                // No need to continue replicating records anymore.
                // We are already over eviction threshold, each put record will cause another eviction.
                recordStore.evictEntries(dataKey);
                break;
            }

            if (++count % DISPOSE_AT_COUNT == 0) {
                recordStore.disposeDeferredBlocks();
            }

        } while (!keyRecordExpiry.isEmpty());

        recordStore.disposeDeferredBlocks();
    }

    // owned or backup
    private void putOrUpdateReplicatedDataWithPerNodeEviction(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        EvictionConfig evictionConfig = mapContainer.getMapConfig().getEvictionConfig();
        long ownedEntryCountOnThisNode = entryCountOnThisNode(mapContainer);

        int count = 0;
        long nowInMillis = Clock.currentTimeMillis();
        do {
            Data dataKey = (Data) keyRecordExpiry.poll();
            Record record = (Record) keyRecordExpiry.poll();
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.poll();

            if (ownedEntryCountOnThisNode >= evictionConfig.getSize()) {
                if (getReplicaIndex() == 0) {
                    recordStore.doPostEvictionOperations(dataKey, record.getValue(), ExpiryReason.NOT_EXPIRED);
                }
            } else {
                Indexes indexes = mapContainer.getIndexes(recordStore.getPartitionId());

                recordStore.putOrUpdateReplicatedRecord(dataKey, record, expiryMetadata,
                        indexesMustBePopulated(indexes), nowInMillis);

                ownedEntryCountOnThisNode++;
            }

            if (++count % DISPOSE_AT_COUNT == 0) {
                recordStore.disposeDeferredBlocks();
            }
        } while (!keyRecordExpiry.isEmpty());

        recordStore.disposeDeferredBlocks();
    }

    private long entryCountOnThisNode(MapContainer mapContainer) {
        int replicaIndex = getReplicaIndex();
        long owned = 0;
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (replicaIndex == 0 ? partitionService.isPartitionOwner(partitionId)
                    : !partitionService.isPartitionOwner(partitionId)) {
                RecordStore store = mapServiceContext.getExistingRecordStore(partitionId, mapContainer.getName());
                if (store != null) {
                    owned += store.size();
                }
            }
        }

        return owned;
    }

    private static boolean hasPerNodeEviction(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        EvictionConfig evictionConfig = mapContainer
                .getMapConfig().getEvictionConfig();
        return mapContainer.getEvictor() != Evictor.NULL_EVICTOR
                && evictionConfig.getMaxSizePolicy() == PER_NODE;
    }

    private void applyNearCacheState(RecordStore recordStore) {
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator(recordStore);
        int partitionId = getPartitionId();

        if (partitionUuid != null) {
            metaDataGenerator.setUuid(partitionId, partitionUuid);
        }

        metaDataGenerator.setCurrentSequence(recordStore.getName(), partitionId, currentSequence);
    }

    private void applyWriteBehindState(RecordStore recordStore) {
        if (!hasWriteBehindState) {
            return;
        }
        WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
        mapDataStore.getTxnReservedCapacityCounter().putAll(counterByTxnId);

        mapDataStore.reset();
        mapDataStore.setFlushSequences(sequences);

        for (DelayedEntry delayedEntry : delayedEntriesList) {
            mapDataStore.addForcibly(delayedEntry);
            mapDataStore.setSequence(delayedEntry.getSequence());
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
    protected final void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeBoolean(firstChunk);
        if (firstChunk) {
            writeMetadata(out);
        }
        context.beforeOperation();
        try {
            writeChunk(out, context);

            Iterator<Map.Entry<Data, Record>> iterator = context.getIterator();
            lastChunk = !iterator.hasNext();
            // make it visible to other threads
            context.setIterator(iterator);
        } finally {
            context.afterOperation();
        }
        out.writeBoolean(lastChunk);
    }

    protected void writeMetadata(ObjectDataOutput out) throws IOException {
        out.writeObject(context.createMapIndexInfo());
        out.writeBoolean(context.isRecordStoreLoaded());
        context.getStats().writeData(out);

        writeWriteBehindState(out, context.getRecordStore());
        writeNearCacheState(out);
    }

    public final void writeNearCacheState(ObjectDataOutput out) throws IOException {
        MetaDataGenerator metaData = getPartitionMetaDataGenerator(context.getRecordStore());
        int partitionId = context.getPartitionId();
        UUID partitionUuid = metaData.getOrCreateUuid(partitionId);

        boolean nullUuid = partitionUuid == null;
        out.writeBoolean(nullUuid);
        if (!nullUuid) {
            out.writeLong(partitionUuid.getMostSignificantBits());
            out.writeLong(partitionUuid.getLeastSignificantBits());
        }

        long currentSequence = metaData.currentSequence(context.getMapName(), partitionId);
        out.writeLong(currentSequence);
    }

    public final void readNearCacheState(ObjectDataInput in) throws IOException {
        boolean nullUuid = in.readBoolean();
        partitionUuid = nullUuid ? null : new UUID(in.readLong(), in.readLong());
        currentSequence = in.readLong();
    }

    private MetaDataGenerator getPartitionMetaDataGenerator(RecordStore recordStore) {
        MapServiceContext mapServiceContext = recordStore.getMapContainer().getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }

    private void writeWriteBehindState(ObjectDataOutput out, RecordStore recordStore) throws IOException {
        MapContainer mapContainer = recordStore.getMapContainer();
        MapConfig mapConfig = mapContainer.getMapConfig();
        if (mapConfig.getTotalBackupCount() < getReplicaIndex()
                || !mapContainer.getMapStoreContext().isWriteBehindMapStoreEnabled()) {
            // we don't have hasWriteBehindState
            out.writeBoolean(false);
            return;
        }

        // we have hasWriteBehindState
        out.writeBoolean(true);

        MapServiceContext mapServiceContext = recordStore.getMapContainer().getMapServiceContext();
        WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();

        // write delayed entries
        List<DelayedEntry> delayedEntries = mapDataStore.getWriteBehindQueue().asList();
        out.writeInt(delayedEntries.size());
        for (DelayedEntry e : delayedEntries) {
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
        // write sequences
        Deque<WriteBehindStore.Sequence> sequences = new ArrayDeque<>(mapDataStore.getFlushSequences());
        out.writeInt(sequences.size());
        for (WriteBehindStore.Sequence sequence : sequences) {
            out.writeLong(sequence.getSequence());
            out.writeBoolean(sequence.isFullFlush());
        }
        // write txn reservations
        Map<UUID, Long> reservationsByTxnId = mapDataStore
                .getTxnReservedCapacityCounter().getReservedCapacityCountPerTxnId();
        out.writeInt(reservationsByTxnId.size());
        for (Map.Entry<UUID, Long> counterByTxnId : reservationsByTxnId.entrySet()) {
            writeUUID(out, counterByTxnId.getKey());
            out.writeLong(counterByTxnId.getValue());
        }
    }

    /**
     * This method writes a map's main data, which is key-value
     * pairs, to output stream.
     * <p>
     * If number of written bytes exceeds chunk limit, it stops writing.
     * Next key-values pairs are written in subsequent chunks later.
     */
    protected final void writeChunk(ObjectDataOutput out, MapChunkContext context) throws IOException {
        SerializationService ss = context.getSerializationService();
        long recordCount = 0;

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
            recordCount++;

            if (isEndOfChunk.getAsBoolean()) {
                break;
            }
        }
        incrementReplicationRecordCount(recordCount);
        if (!entries.hasNext()) {
            incrementReplicationCount();
        }
        // indicates end of chunk
        IOUtil.writeData(out, null);
    }

    @Override
    protected final void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        this.firstChunk = in.readBoolean();
        if (firstChunk) {
            readMetadata(in);
        }

        readChunk(in);

        this.lastChunk = in.readBoolean();
    }

    protected void readMetadata(ObjectDataInput in) throws IOException {
        this.mapIndexInfo = in.readObject();
        this.loaded = in.readBoolean();
        this.stats = new LocalRecordStoreStatsImpl();
        stats.readData(in);

        readWriteBehindState(in);
        readNearCacheState(in);
    }

    private void readWriteBehindState(ObjectDataInput in) throws IOException {
        hasWriteBehindState = in.readBoolean();
        if (!hasWriteBehindState) {
            return;
        }
        // read delayed entries
        int listSize = in.readInt();
        delayedEntriesList = new ArrayList<>(listSize);
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
        // read sequences
        int setSize = in.readInt();
        sequences = new ArrayDeque<>(setSize);
        for (int j = 0; j < setSize; j++) {
            sequences.add(new WriteBehindStore.Sequence(in.readLong(), in.readBoolean()));
        }
        // read txn reservations
        int numOfCounters = in.readInt();
        counterByTxnId = createHashMap(numOfCounters);
        for (int j = 0; j < numOfCounters; j++) {
            counterByTxnId.put(readUUID(in), in.readLong());
        }
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
