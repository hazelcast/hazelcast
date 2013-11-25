package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AtomicReferenceBaseOperation extends Operation implements PartitionAwareOperation {

    protected String name;

    public AtomicReferenceBaseOperation() {
        super();
    }

    public AtomicReferenceBaseOperation(String name) {
        this.name = name;
    }

    public AtomicReferenceWrapper getReference() {
        return ((AtomicReferenceService) getService()).getReference(name);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

}
