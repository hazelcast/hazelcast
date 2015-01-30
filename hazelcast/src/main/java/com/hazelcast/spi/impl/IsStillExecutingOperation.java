package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;

/**
 * An operation that checks if another operation is still running.
 */
public class IsStillExecutingOperation extends AbstractOperation implements UrgentSystemOperation {

    private long operationCallId;

    IsStillExecutingOperation() {
    }

    IsStillExecutingOperation(long operationCallId, int partitionId) {
        this.operationCallId = operationCallId;
        setPartitionId(partitionId);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        BasicOperationScheduler scheduler = operationService.scheduler;
        boolean executing = scheduler.isOperationExecuting(getCallerAddress(), getPartitionId(), operationCallId);
        getResponseHandler().sendResponse(executing);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.operationCallId = in.readLong();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(operationCallId);
    }
}
