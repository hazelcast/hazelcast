package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AlterOperation;
import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class AlterRequest extends AbstractAlterRequest {

    public AlterRequest() {
    }

    public AlterRequest(String name, Data function) {
        super(name, function);
    }

    @Override
    protected Operation prepareOperation() {
        return new AlterOperation(name,function);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.ALTER;
    }
}
