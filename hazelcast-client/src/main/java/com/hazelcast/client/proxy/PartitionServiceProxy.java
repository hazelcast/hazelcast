package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.partition.MigrationListener;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @mdogan 5/16/13
 */
public final class PartitionServiceProxy implements PartitionService {

    private final ClientPartitionService partitionService;

    public PartitionServiceProxy(ClientPartitionService partitionService) {
        this.partitionService = partitionService;
    }

    @Override
    public Set<Partition> getPartitions() {
        final int partitionCount = partitionService.getPartitionCount();
        Set<Partition> partitions = new LinkedHashSet<Partition>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            final Partition partition = partitionService.getPartition(i);
            partitions.add(partition);
        }
        return partitions;
    }

    @Override
    public Partition getPartition(Object key) {
        final int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartition(partitionId);
    }

    @Override
    public String addMigrationListener(MigrationListener migrationListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasOngoingMigration() {
        throw new UnsupportedOperationException();
    }
}
