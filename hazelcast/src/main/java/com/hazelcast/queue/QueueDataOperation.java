package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: ali
 * Date: 11/23/12
 * Time: 3:52 AM
 */
public abstract class QueueDataOperation extends QueueKeyBasedOperation {

    Data data;

    protected QueueDataOperation() {
    }

    public QueueDataOperation(final String name, final Data data){
        super(name);
        this.data = data;
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
