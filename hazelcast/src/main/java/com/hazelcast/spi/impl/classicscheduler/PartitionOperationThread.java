package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.OperationHandler;

/**
 * An {@link OperationThread} that executes Operations for a particular partition, e.g. a map.get operation.
 */
public final class PartitionOperationThread extends OperationThread {

    private final OperationHandler[] partitionOperationHandlers;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    public PartitionOperationThread(String name, int threadId,
                                    ScheduleQueue scheduleQueue, ILogger logger,
                                    HazelcastThreadGroup threadGroup, NodeExtension nodeExtension,
                                    OperationHandler[] partitionOperationHandlers) {
        super(name, threadId, scheduleQueue, logger, threadGroup, nodeExtension);
        this.partitionOperationHandlers = partitionOperationHandlers;
    }

    /**
     * For each partition there is a OperationHandler instance. So we need to find the right one based on the
     * partition-id.
     */
    @Override
    public OperationHandler getOperationHandler(int partitionId) {
        return partitionOperationHandlers[partitionId];
    }
}
