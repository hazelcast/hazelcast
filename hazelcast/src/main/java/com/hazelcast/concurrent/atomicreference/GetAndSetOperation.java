package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class GetAndSetOperation extends AtomicReferenceBackupAwareOperation {

    private Data newValue;
    private Data returnValue;

    public GetAndSetOperation() {
        super();
    }

    public GetAndSetOperation(String name, Data newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().getAndSet(newValue);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readObject();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, newValue);
    }
}

