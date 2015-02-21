package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.util.concurrent.atomic.AtomicInteger;

public class MockPartitionOperation extends AbstractOperation implements PartitionAwareOperation {
    public final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void run() throws Exception {
        counter.incrementAndGet();
    }
}
