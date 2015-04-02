package com.hazelcast.spi;

import com.hazelcast.partition.InternalPartitionLostEvent;

/**
 * An interface that can be implemented by SPI services to get notified of when a partition-related event occurs; for example,
 * if a {@link com.hazelcast.map.impl.MapService} notifies its map listeners when partition is lost for a map.
 *
 * @see com.hazelcast.partition.InternalPartitionLostEvent
 */
public interface PartitionAwareService {

    /**
     * Invoked when a partition lost is detected
     * @param event The event object that contains the partition id and the number of replicas that is lost
     */
    void onPartitionLost(InternalPartitionLostEvent event);

}
