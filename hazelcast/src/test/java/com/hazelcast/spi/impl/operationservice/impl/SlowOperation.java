package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

class SlowOperation extends AbstractOperation {

    Object response;
    long durationMs;

    public SlowOperation() {
    }

    public SlowOperation(long durationMs) {
        this.durationMs = durationMs;
    }

    public SlowOperation(long durationMs, Object response) {
        this.durationMs = durationMs;
        this.response = response;
    }

    @Override
    public void run() throws Exception {
        Thread.sleep(durationMs);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(durationMs);
        out.writeObject(response);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        durationMs = in.readLong();
        response = in.readObject();
    }

}
