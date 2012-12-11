package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 12/11/12
 */
public class RemoveBackupOperation extends QueueOperation implements BackupOperation {

    private Data data;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data data) {
        super(name);
        this.data = data;
    }

    public void run() throws Exception {
        response = getContainer().dataQueue.remove(data);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        data.writeData(out);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        data = new Data();
        data.readData(in);
    }
}
