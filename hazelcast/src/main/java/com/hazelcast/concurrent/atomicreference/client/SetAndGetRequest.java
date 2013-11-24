package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.SetAndGetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class SetAndGetRequest extends ModifyRequest {

    public SetAndGetRequest() {
    }

    public SetAndGetRequest(String name, Data update) {
        super(name, update);
    }

    @Override
    protected Operation prepareOperation() {
        return new SetAndGetOperation(name, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.SET_AND_GET;
    }
}
