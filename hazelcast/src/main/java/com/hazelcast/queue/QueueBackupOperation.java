package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeService;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @ali 12/6/12
 */
public class QueueBackupOperation extends AbstractOperation implements BackupOperation {


    private Data operationData;

    private transient QueueOperation operation;

    public QueueBackupOperation() {
    }

    public QueueBackupOperation(final Data operation) {
        this.operationData = operation;
    }

    public QueueBackupOperation(final QueueOperation operation) {
        this.operationData = IOUtil.toData(operation);
        this.operation = operation;
    }

    
    public void beforeRun() throws Exception {
        final NodeService nodeService = getNodeService();
        operation = (QueueOperation) nodeService.toObject(operationData);
        operation.setNodeService(nodeService)
                .setCaller(getCaller())
                .setCallId(getCallId())
                .setConnection(getConnection())
                .setServiceName(getServiceName())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(getReplicaIndex())
                .setResponseHandler(getResponseHandler());
        operation.beforeRun();
    }

    public void run() throws Exception {
        operation.run();
    }

    public void afterRun() throws Exception {
        operation.afterRun();
    }

    public boolean returnsResponse() {
        return true;
    }

    public Object getResponse() {
        return true;
    }

    public boolean needsBackup() {
        return false;
    }

    protected void writeInternal(final DataOutput out) throws IOException {
        operationData.writeData(out);
    }

    protected void readInternal(final DataInput in) throws IOException {
        operationData = new Data();
        operationData.readData(in);
    }
}
