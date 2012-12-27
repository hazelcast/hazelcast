package com.hazelcast.atomic;

import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// author: sancar - 24.12.2012
public class AddAndGetOperation extends AtomicNumberBackupAwareOperation {

    private long delta;

    private long returnValue;

    public AddAndGetOperation(){
        super();
    }

    public AddAndGetOperation(String name, long delta){
        super(name);
        this.delta = delta;
    }

    @Override
    public void run() throws Exception {
        returnValue = getNumber() + delta;
        setNumber(returnValue);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name,returnValue);
    }
}
