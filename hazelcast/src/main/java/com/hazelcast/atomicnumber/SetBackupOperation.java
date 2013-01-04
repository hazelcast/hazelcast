package com.hazelcast.atomicnumber;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

// author: sancar - 25.12.2012
public class SetBackupOperation extends AtomicNumberBaseOperation implements BackupOperation {

    private long newValue;

    public SetBackupOperation() {
        super();
    }

    public SetBackupOperation(String name, long newValue) {
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

    public Object getResponse() {
        return newValue;
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
}
