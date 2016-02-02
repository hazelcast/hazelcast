package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DummyOperation extends AbstractOperation {
    public Object value;
    public Object result;
    private long sleepMs;

    public DummyOperation() {
    }

    public DummyOperation(Object value) {
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
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

    /**
     * Configures a sleeping period when the {@link #run()} is called.
     *
     * @param sleepMs the time to sleep.
     * @return the this instance for fluent interface.
     */
    public DummyOperation setDelayMs(long sleepMs) {
        this.sleepMs = sleepMs;
        return this;
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
