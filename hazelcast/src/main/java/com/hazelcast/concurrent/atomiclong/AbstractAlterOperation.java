package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.Function;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractAlterOperation extends AtomicLongBackupAwareOperation {

    protected Function<Long,Long> function;
    protected long response;
    protected long backup;

    public AbstractAlterOperation() {
    }

    public AbstractAlterOperation(String name, Function<Long,Long> function) {
        super(name);
        this.function = function;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, backup);
    }
}