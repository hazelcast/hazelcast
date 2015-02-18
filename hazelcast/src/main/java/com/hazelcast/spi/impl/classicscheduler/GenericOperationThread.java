package com.hazelcast.spi.impl.classicscheduler;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.OperationHandler;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    private final OperationHandler operationHandler;

    public GenericOperationThread(String name, int threadId, BlockingQueue workQueue,
                                  Queue priorityWorkQueue, ILogger logger, HazelcastThreadGroup threadGroup,
                                  NodeExtension nodeExtension,  OperationHandler operationHandler) {
        super(name, threadId, workQueue, priorityWorkQueue, logger, threadGroup, nodeExtension);
        this.operationHandler = operationHandler;
    }

    @Override
    public OperationHandler getOperationHandler(int partitionId) {
        return operationHandler;
    }
}
