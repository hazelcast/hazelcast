package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.SetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class SetRequest extends ModifyRequest {

    public SetRequest() {
    }

    public SetRequest(String name, Data update) {
        super(name,update);
    }

    @Override
    protected Operation prepareOperation() {
        return new SetOperation(name, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.SET;
    }
}
