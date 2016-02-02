package com.hazelcast.spi.impl.operationservice.impl;

public class DummyPartitionOperation extends DummyOperation {

    public DummyPartitionOperation() {
        setPartitionId(0);
    }

    public DummyPartitionOperation(Object value) {
        super(value);
        setPartitionId(0);
    }
}
