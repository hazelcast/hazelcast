package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.OperationHandler;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    private final OperationHandler operationHandler;

    public GenericOperationThread(String name, int threadId, ScheduleQueue scheduleQueue,
                                  ILogger logger, HazelcastThreadGroup threadGroup,
                                  NodeExtension nodeExtension,  OperationHandler operationHandler) {
        super(name, threadId, scheduleQueue, logger, threadGroup, nodeExtension);
        this.operationHandler = operationHandler;
    }

    @Override
    public OperationHandler getOperationHandler(int partitionId) {
        return operationHandler;
    }
}
