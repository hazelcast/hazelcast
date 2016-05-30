package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.map.impl.operation.AbstractMultiPartitionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

class DummyMultiPartitionOperation extends AbstractMultiPartitionOperation implements DataSerializable {

    private final AtomicBoolean failureOperationCreated = new AtomicBoolean();

    private Object[] data;

    @SuppressWarnings("unused")
    public DummyMultiPartitionOperation() {
    }

    DummyMultiPartitionOperation(int partition, Object data) {
        this(new int[]{partition}, new Object[]{data});
    }

    private DummyMultiPartitionOperation(int[] partitions, Object[] data) {
        super(HazelcastTestSupport.randomMapName(), partitions);
        this.data = data;
    }

    boolean getFailureOperationCreated() {
        return failureOperationCreated.get();
    }

    @Override
    protected void doRun(int[] partitions, Future[] futures) {
        NodeEngine nodeEngine = getNodeEngine();
        for (int i = 0; i < partitions.length; i++) {
            Operation op = new DummyOperation(data[i]);
            op.setNodeEngine(nodeEngine)
                    .setPartitionId(partitions[i])
                    .setReplicaIndex(getReplicaIndex())
                    .setService(getService())
                    .setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(op, getCallerAddress());
            futures[i] = nodeEngine.getOperationService().invokeOnPartition(op);
        }
    }

    @Override
    public Operation createFailureOperation(int failedPartitionId, int partitionIndex) {
        failureOperationCreated.set(true);
        return new DummyOperation(data[partitionIndex]);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(data.length);
        for (Object aData : data) {
            out.writeObject(aData);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        data = new Object[size];
        for (int i = 0; i < size; i++) {
            data[i] = in.readObject();
        }
    }
}
