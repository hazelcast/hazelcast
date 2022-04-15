package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.impl.engine.CircularQueue;
import com.hazelcast.spi.impl.engine.Reactor;
import com.hazelcast.spi.impl.engine.Scheduler;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_REQUEST_PAYLOAD;
import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_RESPONSE_PAYLOAD;
import static com.hazelcast.spi.impl.requestservice.Op.BLOCKED;
import static com.hazelcast.spi.impl.requestservice.Op.COMPLETED;

public final class OpScheduler implements Scheduler {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter ticks = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final CircularQueue<Op> runQueue;
    private final int batchSize;
    private final FrameAllocator localResponseFrameAllocator;
    private final FrameAllocator remoteResponseFrameAllocator;
    private final OpAllocator opAllocator;
    private Reactor reactor;

    public OpScheduler(int capacity,
                       int batchSize,
                       Managers managers,
                       FrameAllocator localResponseFrameAllocator,
                       FrameAllocator remoteResponseFrameAllocator) {
        this.runQueue = new CircularQueue<>(capacity);
        this.batchSize = batchSize;
        this.localResponseFrameAllocator = localResponseFrameAllocator;
        this.remoteResponseFrameAllocator = remoteResponseFrameAllocator;
        this.opAllocator = new OpAllocator(this, managers);
    }

    @Override
    public void setReactor(Reactor reactor) {
        this.reactor = reactor;
    }

    public Reactor getReactor() {
        return reactor;
    }

    @Override
    public void schedule(Frame request) {
        Op op = opAllocator.allocate(request);
        op.response = request.future != null
                ? remoteResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD)
                : localResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD);
        op.request = request.position(OFFSET_REQUEST_PAYLOAD);
        schedule(op);
    }

    public void schedule(Op op) {
        scheduled.inc();

        if (!runQueue.offer(op)) {
            //todo: return false and send some kind of rejection message
            throw new RuntimeException("Scheduler overloaded");
        }

        runSingle();
    }

    public boolean tick() {
        ticks.inc();

        for (int k = 0; k < batchSize - 1; k++) {
            if (!runSingle()) {
                return false;
            }
        }

        return runSingle();
    }

    public boolean runSingle() {
        Op op = runQueue.poll();
        if (op == null) {
            return false;
        }

        try {
            int runCode = op.run();
            switch (runCode) {
                case COMPLETED:
                    completed.inc();
                    if (op.request.future == null) {
                        op.request.channel.unsafeWriteAndFlush(op.response);
                    } else {
                        op.request.future.complete(op.response);
                        op.response.release();
                    }
                    op.request.release();
                    op.release();
                    break;
                case BLOCKED:
                    break;
                default:
                    throw new RuntimeException();
            }

            return !runQueue.isEmpty();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
