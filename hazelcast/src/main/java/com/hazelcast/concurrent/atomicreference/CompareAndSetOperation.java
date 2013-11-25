package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CompareAndSetOperation extends AtomicReferenceBackupAwareOperation {

    private Data expect;
    private Data update;
    private boolean returnValue = false;

    public CompareAndSetOperation() {
    }

    public CompareAndSetOperation(String name, Data expect, Data update) {
        super(name);
        this.expect = expect;
        this.update = update;
    }

    @Override
    public void run() throws Exception {
        returnValue = getReference().compareAndSet(expect, update);
        shouldBackup = !returnValue;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expect);
        out.writeObject(update);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readObject();
        update = in.readObject();
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, update);
    }
}

