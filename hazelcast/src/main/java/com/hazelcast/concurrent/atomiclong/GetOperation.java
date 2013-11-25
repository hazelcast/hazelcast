package com.hazelcast.concurrent.atomiclong;

public class GetOperation extends AtomicLongBaseOperation {

    private long returnValue;

    public GetOperation() {
        super();
    }

    public GetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getNumber().get();
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }
}
