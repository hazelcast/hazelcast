package com.hazelcast.map;

import com.hazelcast.map.operation.MapReplicationOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;

/**
 * Contains map migration support.
 */
abstract class MapMigrationSupport extends MapSplitBrainSupport implements MigrationAwareService {

    protected MapMigrationSupport(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        final MapReplicationOperation operation
                = new MapReplicationOperation(getService(), container, event.getPartitionId(), event.getReplicaIndex());
        operation.setService(getService());
        return operation.isEmpty() ? null : operation;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrateIndex(event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionData(event.getPartitionId());
        }
        ownedPartitions.set(getMemberPartitions());
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionData(event.getPartitionId());
        }
        ownedPartitions.set(getMemberPartitions());
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearPartitionData(partitionId);
    }

    private void migrateIndex(PartitionMigrationEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = getMapContainer(recordStore.getName());
            final IndexService indexService = mapContainer.getIndexService();
            if (indexService.hasIndex()) {
                for (Record record : recordStore.getReadonlyRecordMap().values()) {
                    if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                        indexService.removeEntryIndex(record.getKey());
                    } else {
                        Object value = record.getValue();
                        if (value != null) {
                            indexService.saveEntryIndex(new QueryEntry(getSerializationService(), record.getKey(),
                                    record.getKey(), value));
                        }
                    }
                }
            }
        }
    }

}
