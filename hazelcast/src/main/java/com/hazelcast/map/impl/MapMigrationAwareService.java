/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.flushAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.removeAccumulator;
import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.spi.partition.MigrationEndpoint.SOURCE;

/**
 * Defines migration behavior of map service.
 *
 * @see MapService
 */
class MapMigrationAwareService implements FragmentedMigrationAwareService {

    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;

    MapMigrationAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        return container.getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace && MapService.SERVICE_NAME.equals(namespace.getServiceName());
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
            populateIndexes(event, TargetIndexes.NON_GLOBAL);
        }

        flushAndRemoveQueryCaches(event);
    }

    /**
     * Flush and remove query cache on this source partition.
     */
    private void flushAndRemoveQueryCaches(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() != MigrationEndpoint.SOURCE) {
            return;
        }

        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        int partitionId = event.getPartitionId();

        flushAccumulator(publisherContext, partitionId);
        removeAccumulator(publisherContext, partitionId);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);

        Operation operation = new MapReplicationOperation(container, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {

        assert assertAllKnownNamespaces(namespaces);

        int partitionId = event.getPartitionId();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);

        Operation operation = new MapReplicationOperation(container, namespaces, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    private boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace) : namespace + " is not a MapService namespace!";
        }
        return true;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            populateIndexes(event, TargetIndexes.GLOBAL);
        } else {
            depopulateIndexes(event);
        }

        if (SOURCE == event.getMigrationEndpoint()) {
            clearMapsHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : partitionContainer.getAllRecordStores()) {
            // in case the record store has been created without loading during migration trigger again
            // if loading has been already started this call will do nothing
            recordStore.startLoading();
        }
        mapServiceContext.reloadOwnedPartitions();

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
            clearMapsHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        }

        mapServiceContext.reloadOwnedPartitions();
    }

    private void clearNonGlobalIndexes(PartitionMigrationEvent event) {
        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.hasIndex() || indexes.isGlobal()) {
                // no indexes to work with
                continue;
            }

            indexes.clearContents();
        }
    }

    private void clearMapsHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.clearPartitionData(partitionId);
        } else {
            mapServiceContext.clearMapsHavingLesserBackupCountThan(partitionId, thresholdReplicaIndex);
        }
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return mapServiceContext.getMapNearCacheManager().getInvalidator().getMetaDataGenerator();
    }

    private void populateIndexes(PartitionMigrationEvent event, TargetIndexes targetIndexes) {
        assert event.getMigrationEndpoint() == DESTINATION;
        assert targetIndexes != null;

        if (event.getNewReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return;
        }

        final long now = getNow();

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());

            // RU_COMPAT_3_9
            // Old nodes (3.9-) won't send mapIndexInfos to new nodes (3.10+) in the map-replication operation.
            // This is the reason why we pick up the mapContainer.getIndexesToAdd() that were added by the
            // PostJoinMapOperation and we add them to the map, before we add data
            addPartitionIndexes(recordStore, event.getPartitionId(), mapContainer.getPartitionIndexesToAdd());

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.hasIndex()) {
                // no indexes to work with
                continue;
            }

            if (indexes.isGlobal() && targetIndexes == TargetIndexes.NON_GLOBAL) {
                continue;
            }
            if (!indexes.isGlobal() && targetIndexes == TargetIndexes.GLOBAL) {
                continue;
            }

            final Iterator<Record> iterator = recordStore.iterator(now, false);
            while (iterator.hasNext()) {
                final Record record = iterator.next();
                final Data key = record.getKey();

                final Object value = Records.getValueOrCachedValue(record, serializationService);
                if (value != null) {
                    QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
                    indexes.saveEntryIndex(queryEntry, null);
                }
            }
        }
    }

    private void depopulateIndexes(PartitionMigrationEvent event) {
        assert event.getMigrationEndpoint() == SOURCE;
        assert event.getNewReplicaIndex() != 0 : "Invalid migration event: " + event;

        if (event.getCurrentReplicaIndex() != 0) {
            // backup partitions have no indexes to depopulate
            return;
        }

        final long now = getNow();

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.hasIndex()) {
                // no indexes to work with
                continue;
            }

            final Iterator<Record> iterator = recordStore.iterator(now, false);
            while (iterator.hasNext()) {
                final Record record = iterator.next();
                final Data key = record.getKey();

                final Object value = Records.getValueOrCachedValue(record, serializationService);
                indexes.removeEntryIndex(key, value);
            }
        }
    }

    // RU_COMPAT_3_9
    private void addPartitionIndexes(RecordStore recordStore, int partitionId, Collection<IndexInfo> indexInfos) {
        if (indexInfos == null) {
            return;
        }
        MapContainer mapContainer = recordStore.getMapContainer();
        if (!mapContainer.isGlobalIndexEnabled()) {
            Indexes indexes = mapContainer.getIndexes(partitionId);
            for (IndexInfo indexInfo : indexInfos) {
                indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
            }
        }
    }

    private enum TargetIndexes {
        GLOBAL, NON_GLOBAL
    }

    private static boolean isLocalPromotion(PartitionMigrationEvent event) {
        return event.getMigrationEndpoint() == DESTINATION && event.getCurrentReplicaIndex() > 0
                && event.getNewReplicaIndex() == 0;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
