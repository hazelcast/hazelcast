package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DummyOperation extends AbstractOperation {
    public Object value;
    public Object result;

    public DummyOperation() {
    }

    public DummyOperation(Object value) {
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        if (value instanceof Runnable) {
            ((Runnable)value).run();
            result = value;
        }else if(value instanceof Callable){
            result = ((Callable)value).call();
        }else{
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
