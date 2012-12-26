package com.hazelcast.atomicNumber;

import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// author: sancar - 24.12.2012
public class GetAndSetOperation extends AtomicNumberBackupAwareOperation {

    private long newValue;

    private long returnValue;

    public GetAndSetOperation(){
        super();
    }

    public GetAndSetOperation(String name, long newValue){
        super(name);
        this.newValue = newValue;
    }

    @Override
    public void run() throws Exception {

        returnValue = getNumber();
        setNumber(newValue);
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
        out.writeLong(newValue);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readLong();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name,newValue);
    }
}
