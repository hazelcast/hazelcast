package com.hazelcast.spi.impl;

public interface OperationHandlerFactory {

    OperationHandler createPartitionHandler(int partitionId);

    OperationHandler createGenericOperationHandler();

    OperationHandler createAdHocOperationHandler();
}
