package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractAlterOperation extends AtomicLongBackupAwareOperation {

    protected Data function;
    protected long response;
    protected long backup;

    public AbstractAlterOperation() {
        super();
    }

    public AbstractAlterOperation(String name, Data function) {
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