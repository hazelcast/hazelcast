package com.hazelcast.partition;

import com.hazelcast.core.MigrationEvent;

/**
 * PartitionEvent is a base interface for partition-related events
 * @see MigrationEvent
 * @see PartitionLostEvent
 */
public interface PartitionEvent {

    /**
     * Partition id that the event is dispatch for
     * @return partition id that the event is dispatch for
     */
    int getPartitionId();

}
