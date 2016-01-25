/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.Clock;

import java.util.Iterator;

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
        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        final MapReplicationOperation operation
                = new MapReplicationOperation(mapServiceContext.getService(), container,
                event.getPartitionId(), event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        return operation;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrateIndex(event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            mapServiceContext.clearPartitionData(event.getPartitionId());
        }
        mapServiceContext.reloadOwnedPartitions();
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            mapServiceContext.clearPartitionData(event.getPartitionId());
        }
        mapServiceContext.reloadOwnedPartitions();
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        mapServiceContext.clearPartitionData(partitionId);
    }

    protected void migrateIndex(PartitionMigrationEvent event) {
        final long now = getNow();

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());
            final Indexes indexes = mapContainer.getIndexes();
            if (indexes.hasIndex()) {
                final Iterator<Record> iterator = recordStore.iterator(now, false);
                while (iterator.hasNext()) {
                    Record record = iterator.next();
                    Data key = record.getKey();
                    if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                        Object value = Records.getValueOrCachedValue(record, serializationService);
                        indexes.removeEntryIndex(key, value);
                    } else {
                        Object value = Records.getValueOrCachedValue(record, serializationService);
                        if (value != null) {
                            QueryableEntry queryEntry = mapContainer.newQueryEntry(record.getKey(), value);
                            indexes.saveEntryIndex(queryEntry, null);
                        }
                    }
                }
            }
        }
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

}
