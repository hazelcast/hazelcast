package com.hazelcast.spi.impl.operationexecutor.progressive;

public class PartitionOperation<E> extends DummyOperation<E> {

    public PartitionOperation() {
        this(0);
    }

    public PartitionOperation(int partitionId) {
        super(partitionId);
    }
}
