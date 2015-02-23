package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

/**
 * An {@link OperationThread} that executes Operations for a particular partition, e.g. a map.get operation.
 */
public final class PartitionOperationThread extends OperationThread {

    private final OperationRunner[] partitionOperationRunners;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    public PartitionOperationThread(String name, int threadId,
                                    ScheduleQueue scheduleQueue, ILogger logger,
                                    HazelcastThreadGroup threadGroup, NodeExtension nodeExtension,
                                    OperationRunner[] partitionOperationRunners) {
        super(name, threadId, scheduleQueue, logger, threadGroup, nodeExtension);
        this.partitionOperationRunners = partitionOperationRunners;
    }

    /**
     * For each partition there is a {@link com.hazelcast.spi.impl.operationexecutor.OperationRunner} instance. So we need to
     * find the right one based on the partition-id.
     */
    @Override
    public OperationRunner getOperationRunner(int partitionId) {
        return partitionOperationRunners[partitionId];
    }
}
