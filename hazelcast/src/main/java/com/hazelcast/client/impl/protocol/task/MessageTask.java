package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * Interface for all client message tasks to implement
 */
public interface MessageTask extends PartitionSpecificRunnable {
}
