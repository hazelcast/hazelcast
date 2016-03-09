package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * VoidOperation is an operation that doesn't return a response. This is practical for testing heartbeats and timeouts
 * when the caller is going to wait for a response.
 */
class VoidOperation extends AbstractOperation implements PartitionAwareOperation {

    private long durationMs;

    public VoidOperation() {
    }

    public VoidOperation(long durationMs) {
        this.durationMs = durationMs;
    }

    @Override
    public void run() throws InterruptedException {
        Thread.sleep(durationMs);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(durationMs);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        durationMs = in.readLong();
    }
}
