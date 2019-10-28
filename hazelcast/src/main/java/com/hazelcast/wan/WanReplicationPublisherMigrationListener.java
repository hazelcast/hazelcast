package com.hazelcast.wan;

import com.hazelcast.spi.partition.PartitionMigrationEvent;

/**
 * Interface for WAN publisher migration related events. Can be implemented
 * by WAN publishers to listen to migration events, for example to maintain
 * the WAN event counters.
 * <p>
 * None of the methods of this interface is expected to block or fail.
 * NOTE: used only in Hazelcast Enterprise.
 *
 * @see PartitionMigrationEvent
 * @see com.hazelcast.spi.partition.MigrationAwareService
 */
public interface WanReplicationPublisherMigrationListener {
    /**
     * Indicates that migration started for a given partition
     *
     * @param event the migration event
     */
    void onMigrationStart(PartitionMigrationEvent event);

    /**
     * Indicates that migration is committing for a given partition
     *
     * @param event the migration event
     */
    void onMigrationCommit(PartitionMigrationEvent event);

    /**
     * Indicates that migration is rolling back for a given partition
     *
     * @param event the migration event
     */
    void onMigrationRollback(PartitionMigrationEvent event);
}
