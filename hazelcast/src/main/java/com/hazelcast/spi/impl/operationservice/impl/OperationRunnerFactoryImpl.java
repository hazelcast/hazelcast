package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;

class OperationRunnerFactoryImpl implements OperationRunnerFactory {
    private OperationServiceImpl operationService;

    public OperationRunnerFactoryImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    @Override
    public OperationRunner createAdHocRunner() {
        return new OperationRunnerImpl(operationService, OperationRunnerImpl.AD_HOC_PARTITION_ID);
    }

    @Override
    public OperationRunner createPartitionRunner(int partitionId) {
        return new OperationRunnerImpl(operationService, partitionId);
    }

    @Override
    public OperationRunner createGenericRunner() {
        return new OperationRunnerImpl(operationService, Operation.GENERIC_PARTITION_ID);
    }
}
