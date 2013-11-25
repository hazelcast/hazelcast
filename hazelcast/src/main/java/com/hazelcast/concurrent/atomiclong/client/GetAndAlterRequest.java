package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.concurrent.atomiclong.GetAndAlterOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class GetAndAlterRequest extends AbstractAlterRequest {

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
        return AtomicLongPortableHook.GET_AND_ALTER;
    }
}

