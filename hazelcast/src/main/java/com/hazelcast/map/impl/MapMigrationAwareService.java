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

package com.hazelcast.map.impl;

import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

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
class MapMigrationAwareService implements MigrationAwareService {

    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;

    MapMigrationAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);

        MapReplicationOperation operation = new MapReplicationOperation(container, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());

        return operation;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrateIndex(event);

        if (SOURCE == event.getMigrationEndpoint()) {
            clearMapsHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        } else if (DESTINATION == event.getMigrationEndpoint()) {
            if (event.getNewReplicaIndex() != 0) {
                getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            }
        }

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : partitionContainer.getAllRecordStores()) {
            // in case the record store has been created without loading during migration trigger again
            // if loading has been already started this call will do nothing
            recordStore.startLoading();
        }
        mapServiceContext.reloadOwnedPartitions();

        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int partitionId = event.getPartitionId();
            flushAccumulator(publisherContext, partitionId);
            removeAccumulator(publisherContext, partitionId);
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

    private void migrateIndex(PartitionMigrationEvent event) {
        final long now = getNow();

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());
            final Indexes indexes = mapContainer.getIndexes();
            if (!indexes.hasIndex()) {
                continue;
            }

            final Iterator<Record> iterator = recordStore.iterator(now, false);
            while (iterator.hasNext()) {
                Record record = iterator.next();
                Data key = record.getKey();
                if (event.getMigrationEndpoint() == SOURCE) {
                    assert event.getNewReplicaIndex() != 0 : "Invalid migration event: " + event;
                    Object value = Records.getValueOrCachedValue(record, serializationService);
                    indexes.removeEntryIndex(key, value);
                } else if (event.getNewReplicaIndex() == 0) {
                    Object value = Records.getValueOrCachedValue(record, serializationService);
                    if (value != null) {
                        QueryableEntry queryEntry = mapContainer.newQueryEntry(record.getKey(), value);
                        indexes.saveEntryIndex(queryEntry, null);
                    }
                }
            }
        }
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

}
