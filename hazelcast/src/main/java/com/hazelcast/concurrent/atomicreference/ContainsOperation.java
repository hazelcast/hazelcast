package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class ContainsOperation extends AtomicReferenceBaseOperation {

    private boolean returnValue;
    private Data contains;

    public ContainsOperation() {
        super();
    }

    public ContainsOperation(String name, Data contains) {
        super(name);
        this.contains = contains;
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().contains(contains);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(contains);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        contains = in.readObject();
    }
}
