package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DummyOperation extends AbstractOperation {
    public Object value;
    public Object result;
    private int delayMillis;

    public DummyOperation() {
    }

    public DummyOperation(Object value) {
        this.value = value;
    }

    public DummyOperation setDelayMillis(int delayMillis) {
        this.delayMillis = delayMillis;
        return this;
    }

    @Override
    public void run() throws Exception {
        if(delayMillis>0){
            Thread.sleep(delayMillis);
        }

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
        out.writeInt(delayMillis);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readObject();
        delayMillis = in.readInt();
    }
}
