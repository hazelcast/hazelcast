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

package com.hazelcast.map.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.ChunkSuppliers;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static com.hazelcast.config.CacheDeserializedValues.NEVER;
import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.internal.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.flushAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.removeAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.sendEndOfSequenceEvents;

/**
 * Defines migration behavior of map service.
 *
 * @see MapService
 */
class MapMigrationAwareService
        implements ChunkedMigrationAwareService, OffloadedReplicationPreparation {

    protected final PartitionContainer[] containers;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;

    private final ILogger logger;

    MapMigrationAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.containers = mapServiceContext.getPartitionContainers();
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace
                && MapService.SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (isLocalPromotion(event)) {
            // It's a local partition promotion. We need to populate non-global indexes here since
            // there is no map replication performed in this case. Global indexes are populated
            // during promotion finalization phase.

            // 1. Defensively clear possible stale leftovers from the previous failed promotion attempt.
            clearNonGlobalIndexes(event);

            // 2. Populate non-global partitioned indexes.
            populateIndexes(event, TargetIndexes.NON_GLOBAL, "beforeMigration");
        }

        flushAndRemoveQueryCaches(event);
    }

    /**
     * Flush and remove query cache on this source partition.
     */
    private void flushAndRemoveQueryCaches(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            flushAccumulator(publisherContext, partitionId);
            removeAccumulator(publisherContext, partitionId);
            return;
        }

        if (isLocalPromotion(event)) {
            removeAccumulator(publisherContext, partitionId);
            sendEndOfSequenceEvents(publisherContext, partitionId);
            return;
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event,
                containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        assert assertAllKnownNamespaces(namespaces);

        int partitionId = event.getPartitionId();

        Operation operation = new MapReplicationOperation(containers[partitionId],
                namespaces, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event,
                                          Collection<ServiceNamespace> namespaces) {
        List<ChunkSupplier> chain = new ArrayList<>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            chain.add(new MapChunkSupplier(mapServiceContext, namespace,
                    event.getPartitionId(), event.getReplicaIndex()));
        }

        return ChunkSuppliers.newChainedChunkSupplier(chain);
    }

    boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace)
                    : namespace + " is not a MapService namespace!";
        }
        return true;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            populateIndexes(event, TargetIndexes.GLOBAL, "commitMigration");
        } else {
            depopulateIndexes(event, "commitMigration");
        }

        if (SOURCE == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());
        }

        PartitionContainer partitionContainer
                = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : partitionContainer.getAllRecordStores()) {
            // in case the record store has been created without
            // loading during migration trigger again if loading
            // has been already started this call will do nothing
            recordStore.startLoading();
        }
        mapServiceContext.nullifyOwnedPartitions();

        removeOrRegenerateNearCacheUuid(event);
    }

    private void removeOrRegenerateNearCacheUuid(PartitionMigrationEvent event) {
        if (SOURCE == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
            return;
        }

        if (DESTINATION == event.getMigrationEndpoint() && event.getNewReplicaIndex() != 0) {
            getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            return;
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (DESTINATION == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        }

        mapServiceContext.nullifyOwnedPartitions();
    }

    private void clearNonGlobalIndexes(PartitionMigrationEvent event) {
        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = recordStore.getMapContainer();

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.haveAtLeastOneIndex() || indexes.isGlobal()) {
                // no indexes to work with
                continue;
            }

            indexes.clearAll();
        }
    }

    private void removeRecordStoresHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(recordStore -> true, partitionId,
                    false, true);
        } else {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(lesserBackupMapsThen(thresholdReplicaIndex),
                    partitionId, false, true);
        }
    }

    /**
     * Removes write-behind-queue-reservation-counters inside
     * supplied partition from matching record-stores.
     */
    private void removeWbqCountersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    recordStore -> true, partitionId);
        } else {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    lesserBackupMapsThen(thresholdReplicaIndex), partitionId);
        }
    }

    /**
     * @param backupCount number of backups of a maps' partition
     * @return predicate to find all map partitions which are expected to have
     * fewer backups than given backupCount.
     */
    private static Predicate<RecordStore> lesserBackupMapsThen(final int backupCount) {
        return recordStore -> recordStore.getMapContainer().getTotalBackupCount() < backupCount;
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return mapServiceContext.getMapNearCacheManager().getInvalidator().getMetaDataGenerator();
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    private void populateIndexes(PartitionMigrationEvent event,
                                 TargetIndexes targetIndexes, String stepName) {
        assert event.getMigrationEndpoint() == DESTINATION;
        assert targetIndexes != null;

        if (event.getNewReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return;
        }

        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            MapContainer mapContainer = recordStore.getMapContainer();

            Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            indexes.createIndexesFromRecordedDefinitions();
            if (!indexes.haveAtLeastOneIndex()) {
                // no indexes to work with
                continue;
            }

            if (indexes.isGlobal() && targetIndexes == TargetIndexes.NON_GLOBAL) {
                continue;
            }
            if (!indexes.isGlobal() && targetIndexes == TargetIndexes.GLOBAL) {
                continue;
            }

            InternalIndex[] indexesSnapshot = indexes.getIndexes();

            Indexes.beginPartitionUpdate(indexesSnapshot);

            CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
            CachedQueryEntry<?, ?> cachedEntry = cacheDeserializedValues == NEVER ? new CachedQueryEntry<>(serializationService,
                    mapContainer.getExtractors()) : null;
            recordStore.forEach((key, record) -> {
                Object value = Records.getValueOrCachedValue(record, serializationService);
                if (value != null) {
                    QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
                    queryEntry.setRecord(record);
                    CachedQueryEntry<?, ?> newEntry =
                            cachedEntry == null ? (CachedQueryEntry<?, ?>) queryEntry : cachedEntry.init(key, value);
                    indexes.putEntry(newEntry, null, queryEntry, Index.OperationSource.SYSTEM);
                }
            }, false);

            Indexes.markPartitionAsIndexed(event.getPartitionId(), indexesSnapshot);
        }

        if (logger.isFinestEnabled()) {
            logger.finest(String.format("Populated indexes at step `%s`:[%s]", stepName, event));
        }
    }

    private void depopulateIndexes(PartitionMigrationEvent event, String stepName) {
        assert event.getMigrationEndpoint() == SOURCE;
        assert event.getNewReplicaIndex() != 0 : "Invalid migration event: " + event;

        if (event.getCurrentReplicaIndex() != 0) {
            // backup partitions have no indexes to depopulate
            return;
        }

        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            MapContainer mapContainer = recordStore.getMapContainer();
            Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.haveAtLeastOneIndex()) {
                // no indexes to work with
                continue;
            }

            InternalIndex[] indexesSnapshot = indexes.getIndexes();

            Indexes.beginPartitionUpdate(indexesSnapshot);

            CachedQueryEntry<?, ?> entry = new CachedQueryEntry<>(serializationService, mapContainer.getExtractors());
            recordStore.forEach((key, record) -> {
                Object value = Records.getValueOrCachedValue(record, serializationService);
                entry.init(key, value);
                indexes.removeEntry(entry, Index.OperationSource.SYSTEM);
            }, false);

            Indexes.markPartitionAsUnindexed(event.getPartitionId(), indexesSnapshot);
        }

        if (logger.isFinestEnabled()) {
            logger.finest(String.format("Depopulated indexes at step `%s`:[%s]", stepName, event));
        }
    }

    private enum TargetIndexes {
        GLOBAL, NON_GLOBAL
    }

    public static boolean isLocalPromotion(PartitionMigrationEvent event) {
        return event.getMigrationEndpoint() == DESTINATION && event.getCurrentReplicaIndex() > 0
                && event.getNewReplicaIndex() == 0;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
