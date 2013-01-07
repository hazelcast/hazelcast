package com.hazelcast.atomicnumber;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

// author: sancar - 24.12.2012
public class CompareAndSetOperation extends AtomicNumberBackupAwareOperation {

    private long expect;
    private long update;

    private boolean returnValue = false;

    public CompareAndSetOperation() {
        super();
    }

    public CompareAndSetOperation(String name, long expect, long update) {
        super(name);
        this.expect = expect;
        this.update = update;
    }

    @Override
    public void run() throws Exception {
        if (getNumber() == expect) {
            setNumber(update);
            returnValue = true;
        } else {
            shouldBackup = false;
        }
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
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(expect);
        out.writeLong(update);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expect = in.readLong();
        update = in.readLong();
    }

    public Operation getBackupOperation() {
        return new SetBackupOperation(name, update);
    }
}
