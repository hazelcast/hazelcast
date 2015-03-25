package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.IsStillRunningService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;

public class TraceableIsStillExecutingOperation extends AbstractOperation implements UrgentSystemOperation {

    private String serviceName;
    private Object identifier;

    TraceableIsStillExecutingOperation() {
    }

    public TraceableIsStillExecutingOperation(String serviceName, Object identifier) {
        this.serviceName = serviceName;
        this.identifier = identifier;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        IsStillRunningService isStillRunningService = operationService.getIsStillRunningService();
        boolean executing = isStillRunningService.isOperationExecuting(getCallerAddress(), getCallerUuid(),
                serviceName, identifier);
        getResponseHandler().sendResponse(executing);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        identifier = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeObject(identifier);
    }
}
