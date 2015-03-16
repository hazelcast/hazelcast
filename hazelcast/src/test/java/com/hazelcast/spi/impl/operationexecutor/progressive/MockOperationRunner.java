package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockOperationRunner extends OperationRunner {

    public MockOperationRunner(int partitionId) {
        super(partitionId);
    }


    @Override
    public void run(Packet packet) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void run(Runnable task) {
        task.run();
    }

    @Override
    public void run(Operation task) {
        try {
            task.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
