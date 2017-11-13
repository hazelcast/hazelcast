package com.hazelcast.raft.impl.util;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * A {@link PartitionSpecificRunnable} implementation which wraps a plain {@link Runnable}
 *
 */
public class PartitionSpecificRunnableAdaptor implements PartitionSpecificRunnable {
    private final Runnable task;
    private final int partitionId;

    public PartitionSpecificRunnableAdaptor(Runnable task, int partitionId) {
        this.task = task;
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void run() {
        task.run();
    }

    @Override
    public String toString() {
        return "PartitionSpecificRunnableAdaptor{" + "task=" + task + ", partitionId=" + partitionId + '}';
    }
}
