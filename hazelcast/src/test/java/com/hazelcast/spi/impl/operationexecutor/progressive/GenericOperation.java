package com.hazelcast.spi.impl.operationexecutor.progressive;

class GenericOperation<E> extends DummyOperation<E> {

    public GenericOperation() {
        super(-1);
    }
}
