package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DummyPartitionOperation extends AbstractOperation implements PartitionAwareOperation {
    public Object value;
    public Object result;

    public DummyPartitionOperation() {
        setPartitionId(0);
    }

    public DummyPartitionOperation(Object value) {
        this();
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        if (value instanceof Runnable) {
            ((Runnable) value).run();
            result = value;
        } else if (value instanceof Callable) {
            result = ((Callable) value).call();
        } else {
            result = value;
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readObject();
    }
}
