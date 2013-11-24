package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractAlterOperation extends AtomicReferenceBackupAwareOperation {

    protected Data function;
    protected Object response;
    protected Data update;

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
        return new SetBackupOperation(name, update);
    }
}