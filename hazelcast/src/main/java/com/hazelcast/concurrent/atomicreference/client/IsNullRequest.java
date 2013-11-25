package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.IsNullOperation;
import com.hazelcast.spi.Operation;

public class IsNullRequest extends ReadRequest {

    public IsNullRequest() {
    }

    public IsNullRequest(String name) {
        super(name);
    }

    @Override
    protected Operation prepareOperation() {
        return new IsNullOperation(name);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.IS_NULL;
    }
}