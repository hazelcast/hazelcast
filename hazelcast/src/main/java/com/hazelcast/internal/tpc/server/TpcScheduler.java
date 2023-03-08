package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Scheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationScheduler;

/**
 * The TpcScheduler effectively is chain 2 schedulers:
 * - The OpScheduler for next generation operations
 * - The OperationScheduler for classic operations.
 */
public class TpcScheduler implements Scheduler {

    private final RequestScheduler requestScheduler;
    private final OperationScheduler operationScheduler;

    public TpcScheduler(RequestScheduler requestScheduler, OperationScheduler operationScheduler) {
        this.requestScheduler = requestScheduler;
        this.operationScheduler = operationScheduler;
    }

    @Override
    public void init(Eventloop eventloop) {
        operationScheduler.init(eventloop);
        requestScheduler.init(eventloop);
    }

    @Override
    public boolean tick() {
        boolean hasMore = false;
        hasMore |= requestScheduler.tick();
        hasMore |= operationScheduler.tick();
        return hasMore;
    }

    @Override
    public void schedule(IOBuffer task) {
        requestScheduler.schedule(task);
    }

    @Override
    public void schedule(Object task) {
        if (task instanceof Packet) {
            operationScheduler.schedule(task);
        } else {
            requestScheduler.schedule(task);
        }
    }
}
