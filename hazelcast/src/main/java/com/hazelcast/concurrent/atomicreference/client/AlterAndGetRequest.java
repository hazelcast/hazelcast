package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.AlterAndGetOperation;
import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class AlterAndGetRequest  extends AbstractAlterRequest {

    public AlterAndGetRequest() {
    }

    public AlterAndGetRequest(String name, Data function) {
        super(name, function);
    }

    @Override
    protected Operation prepareOperation() {
        return new AlterAndGetOperation(name,function);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.ALTER_AND_GET;
    }
}
