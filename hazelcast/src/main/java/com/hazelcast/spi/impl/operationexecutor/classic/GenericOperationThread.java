package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    private final OperationRunner operationRunner;

    public GenericOperationThread(String name, int threadId, ScheduleQueue scheduleQueue,
                                  ILogger logger, HazelcastThreadGroup threadGroup,
                                  NodeExtension nodeExtension,  OperationRunner operationRunner) {
        super(name, threadId, scheduleQueue, logger, threadGroup, nodeExtension);
        this.operationRunner = operationRunner;
    }

    @Override
    public OperationRunner getOperationRunner(int partitionId) {
        return operationRunner;
    }
}
