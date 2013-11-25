package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.GetAndAlterOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class GetAndAlterRequest  extends AbstractAlterRequest {

    public GetAndAlterRequest() {
    }

    public GetAndAlterRequest(String name, Data function) {
        super(name, function);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAndAlterOperation(name,function);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.GET_AND_ALTER;
    }
}

