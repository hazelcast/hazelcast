package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.AlterOperation;
import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.core.Function;
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
        return new AlterOperation(name, getFunction());
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.ALTER;
    }
}
