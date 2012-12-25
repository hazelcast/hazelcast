package com.hazelcast.atomicNumber;

import com.hazelcast.spi.BackupOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// author: sancar - 25.12.2012
public class SetBackupOperation extends AtomicNumberBaseOperation  implements BackupOperation {

    private long newValue;

    public SetBackupOperation(){
        super();
    }

    public SetBackupOperation(String name, long newValue){
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
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(newValue);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        newValue = in.readLong();
    }
}
