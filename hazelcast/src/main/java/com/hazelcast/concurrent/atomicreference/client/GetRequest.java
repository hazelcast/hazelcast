package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.GetOperation;
import com.hazelcast.spi.Operation;

public class GetRequest extends ReadRequest {

    public GetRequest() {
    }

    public GetRequest(String name) {
        super(name);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetOperation(name);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.GET;
    }
}