package com.hazelcast.concurrent.atomicreference;

public class IsNullOperation extends AtomicReferenceBaseOperation {

    private boolean returnValue;

    public IsNullOperation() {
        super();
    }

    public IsNullOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().isNull();
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }
}