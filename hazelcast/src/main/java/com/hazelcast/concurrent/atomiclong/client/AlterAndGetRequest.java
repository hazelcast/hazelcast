package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.AlterAndGetOperation;
import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class AlterAndGetRequest extends AbstractAlterRequest {

    public AlterAndGetRequest() {
    }

    public AlterAndGetRequest(String name, Data function) {
        super(name, function);
    }

    @Override
    protected Operation prepareOperation() {
        return new AlterAndGetOperation(name, getFunction());
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.ALTER_AND_GET;
    }
}
