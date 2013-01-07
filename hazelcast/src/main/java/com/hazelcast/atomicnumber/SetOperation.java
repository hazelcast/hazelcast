package com.hazelcast.atomicnumber;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

// author: sancar - 24.12.2012
public class SetOperation extends AtomicNumberBackupAwareOperation {

    private long newValue;

    public SetOperation() {
        super();
    }

    public SetOperation(String name, long newValue) {
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {
        setNumber(newValue);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(newValue);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readLong();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, newValue);
    }
}
