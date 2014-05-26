package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.OperationService;
import java.io.IOException;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.OperationServiceMBean}.
 */
public class SerializableOperationServiceBean implements DataSerializable {


    private int responseQueueSize;
    private int operationExecutorQueueSize;
    private int runningOperationsCount;
    private int remoteOperationCount;
    private long executedOperationCount;
    private long operationThreadCount;

    public SerializableOperationServiceBean() {
    }

    public SerializableOperationServiceBean(OperationService os) {
        responseQueueSize = os.getResponseQueueSize();
        operationExecutorQueueSize = os.getOperationExecutorQueueSize();
        runningOperationsCount = os.getRunningOperationsCount();
        remoteOperationCount = os.getRemoteOperationsCount();
        executedOperationCount = os.getExecutedOperationCount();
        operationThreadCount = os.getPartitionOperationThreadCount();
    }

    public int getResponseQueueSize() {
        return responseQueueSize;
    }

    public void setResponseQueueSize(int responseQueueSize) {
        this.responseQueueSize = responseQueueSize;
    }

    public int getOperationExecutorQueueSize() {
        return operationExecutorQueueSize;
    }

    public void setOperationExecutorQueueSize(int operationExecutorQueueSize) {
        this.operationExecutorQueueSize = operationExecutorQueueSize;
    }

    public int getRunningOperationsCount() {
        return runningOperationsCount;
    }

    public void setRunningOperationsCount(int runningOperationsCount) {
        this.runningOperationsCount = runningOperationsCount;
    }

    public int getRemoteOperationCount() {
        return remoteOperationCount;
    }

    public void setRemoteOperationCount(int remoteOperationCount) {
        this.remoteOperationCount = remoteOperationCount;
    }

    public long getExecutedOperationCount() {
        return executedOperationCount;
    }

    public void setExecutedOperationCount(long executedOperationCount) {
        this.executedOperationCount = executedOperationCount;
    }

    public long getOperationThreadCount() {
        return operationThreadCount;
    }

    public void setOperationThreadCount(long operationThreadCount) {
        this.operationThreadCount = operationThreadCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(responseQueueSize);
        out.writeInt(operationExecutorQueueSize);
        out.writeInt(runningOperationsCount);
        out.writeInt(remoteOperationCount);
        out.writeLong(executedOperationCount);
        out.writeLong(operationThreadCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        responseQueueSize = in.readInt();
        operationExecutorQueueSize = in.readInt();
        runningOperationsCount = in.readInt();
        remoteOperationCount = in.readInt();
        executedOperationCount = in.readLong();
        operationThreadCount = in.readLong();
    }
}
