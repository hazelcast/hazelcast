package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;

/**
 * The OperationScheduler is responsible for scheduling operations to be executed locally.
 */
public interface OperationScheduler {

    @Deprecated
    int getRunningOperationCount();

    @Deprecated
    int getOperationExecutorQueueSize();

    @Deprecated
    int getPriorityOperationExecutorQueueSize();

    @Deprecated
    int getResponseQueueSize();

    @Deprecated
    int getPartitionOperationThreadCount();

    @Deprecated
    int getGenericOperationThreadCount();

    @Deprecated
    void dumpPerformanceMetrics(StringBuffer sb);

    void execute(Operation op);

    void execute(Runnable task, int partitionId);

    void execute(Packet packet);

    void shutdown();

    boolean isAllowedToRunInCurrentThread(Operation op);

    boolean isInvocationAllowedFromCurrentThread(Operation op);

    @Deprecated
    boolean isOperationExecuting(Address callerAddress, int partitionId, long operationCallId);
}
