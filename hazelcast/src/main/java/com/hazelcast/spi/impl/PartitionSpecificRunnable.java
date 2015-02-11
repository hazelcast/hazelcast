package com.hazelcast.spi.impl;

/**
 * A {@link java.lang.Runnable} for a specific partition.
 */
public interface PartitionSpecificRunnable extends Runnable {

    /**
     * Returns the partition-id. If the partition-id is smaller than 0, than it isn't specific to
     * a partition.
     *
     * @return the partition-id.
     */
    int getPartitionId();
}
