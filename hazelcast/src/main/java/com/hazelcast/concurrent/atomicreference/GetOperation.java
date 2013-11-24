package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.serialization.Data;

public class GetOperation extends AtomicReferenceBaseOperation {

    private Data returnValue;

    public GetOperation() {
        super();
    }

    public GetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().get();
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }
}
