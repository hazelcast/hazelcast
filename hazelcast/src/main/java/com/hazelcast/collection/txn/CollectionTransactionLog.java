package com.hazelcast.collection.txn;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @ali 9/3/13
 */
public class CollectionTransactionLog implements KeyAwareTransactionLog {

    private long itemId;
    private String name;
    private Operation op;
    private int partitionId;
    private String serviceName;

    public CollectionTransactionLog() {
    }

    public CollectionTransactionLog(long itemId, String name, int partitionId, String serviceName, Operation op) {
        this.itemId = itemId;
        this.name = name;
        this.op = op;
        this.partitionId = partitionId;
        this.serviceName = serviceName;
    }

    public Object getKey() {
        return itemId;
    }

    public Future prepare(NodeEngine nodeEngine) {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        CollectionPrepareOperation operation = new CollectionPrepareOperation(name, itemId, removeOperation);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(serviceName, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future commit(NodeEngine nodeEngine) {
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(serviceName, op, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        CollectionRollbackOperation operation = new CollectionRollbackOperation(name, itemId, removeOperation);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(serviceName, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeUTF(serviceName);
        out.writeObject(op);
    }

    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        name = in.readUTF();
        partitionId = in.readInt();
        serviceName = in.readUTF();
        op = in.readObject();
    }


}
