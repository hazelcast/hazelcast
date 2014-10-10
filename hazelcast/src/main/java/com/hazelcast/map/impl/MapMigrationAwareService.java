package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.Clock;

import java.util.Iterator;
import java.util.List;

/**
 * Defines migration behavior of map service.
 *
 * @see MapService
 */
class MapMigrationAwareService implements MigrationAwareService {

    private final MapServiceContext mapServiceContext;
    private final SerializationService serializationService;

    public MapMigrationAwareService(MapServiceContext mapServiceContext) {
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
        return operation.isEmpty() ? null : operation;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrateIndex(event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            mapServiceContext.clearPartitionData(event.getPartitionId());
        }
        final List<Integer> memberPartitions = mapServiceContext.getMemberPartitions();
        mapServiceContext.ownedPartitions().set(memberPartitions);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            mapServiceContext.clearPartitionData(event.getPartitionId());
        }
        final List<Integer> memberPartitions = mapServiceContext.getMemberPartitions();
        mapServiceContext.ownedPartitions().set(memberPartitions);
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        mapServiceContext.clearPartitionData(partitionId);
    }

    private void migrateIndex(PartitionMigrationEvent event) {
        final long now = getNow();

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());
            final IndexService indexService = mapContainer.getIndexService();
            if (indexService.hasIndex()) {
                final Iterator<Record> iterator = recordStore.iterator(now, false);
                while (iterator.hasNext()) {
                    final Record record = iterator.next();
                    if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                        indexService.removeEntryIndex(record.getKey());
                    } else {
                        Object value = record.getValue();
                        if (value != null) {
                            indexService.saveEntryIndex(new QueryEntry(serializationService, record.getKey(),
                                    record.getKey(), value));
                        }
                    }
                }
            }
        }
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

}
