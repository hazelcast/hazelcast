package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

/**
 * An operation that checks if another operation is still running.
 */
public class IsStillExecutingOperation extends AbstractOperation {

    private long operationCallId;

    IsStillExecutingOperation() {
    }

    IsStillExecutingOperation(long operationCallId) {
        this.operationCallId = operationCallId;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        boolean executing = operationService.isOperationExecuting(getCallerAddress(), getCallerUuid(), operationCallId);
        getResponseHandler().sendResponse(executing);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        operationCallId = in.readLong();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(operationCallId);
    }
}
