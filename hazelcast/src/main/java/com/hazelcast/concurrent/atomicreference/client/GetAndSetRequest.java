package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.GetAndSetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class GetAndSetRequest  extends ModifyRequest {

    public GetAndSetRequest() {
    }

    public GetAndSetRequest(String name, Data update) {
        super(name,update);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAndSetOperation(name, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.GET_AND_SET;
    }
}
