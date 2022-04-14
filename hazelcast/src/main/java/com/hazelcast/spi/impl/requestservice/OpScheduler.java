package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.impl.reactor.CircularQueue;
import com.hazelcast.spi.impl.reactor.Scheduler;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import com.hazelcast.spi.impl.reactor.frame.FrameAllocator;
import com.hazelcast.spi.impl.requestservice.Op;
import org.jetbrains.annotations.Async;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_REQUEST_PAYLOAD;
import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_RESPONSE_PAYLOAD;
import static com.hazelcast.spi.impl.requestservice.Op.COMPLETED;

public final class OpScheduler implements Scheduler {

    private final SwCounter ticks = newSwCounter();
    private final CircularQueue<Op> runQueue;
    private final int batchSize;
    private final Managers managers;
    private final FrameAllocator localResponseFrameAllocator;
    private final FrameAllocator remoteResponseFrameAllocator;
    private OpAllocator opAllocator = new OpAllocator();

    public OpScheduler(int capacity,
                       int batchSize,
                       Managers managers,
                       FrameAllocator localResponseFrameAllocator,
                       FrameAllocator remoteResponseFrameAllocator) {
        this.runQueue = new CircularQueue<>(capacity);
        this.batchSize = batchSize;
        this.managers = managers;
        this.localResponseFrameAllocator = localResponseFrameAllocator;
        this.remoteResponseFrameAllocator = remoteResponseFrameAllocator;
    }

    public void schedule(Frame request){
        //requests.inc();
        Op op = opAllocator.allocate(request);
        op.managers = managers;
        if (request.future == null) {
            op.response = localResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD);
        } else {
            op.response = remoteResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD);
        }
        op.request = request.position(OFFSET_REQUEST_PAYLOAD);
        schedule(op);
    }

    public void schedule(Op op) {
        if (!runQueue.offer(op)) {
            //todo: return false and send some kind of rejection message
            throw new RuntimeException("Scheduler overloaded");
        }

        runSingle();
    }

    public boolean tick() {
        for (int k = 0; k < batchSize - 1; k++) {
            if (!runSingle()) {
                return false;
            }
        }

        return runSingle();
    }

    public boolean runSingle() {
        ticks.inc();

        Op op = runQueue.poll();
        if (op == null) {
            return false;
        }

        try {
            int runCode = op.run();
            switch (runCode) {
                case COMPLETED:
                    if (op.request.future == null) {
                        op.request.channel.unsafeWriteAndFlush(op.response);
                    } else {
                        op.request.future.complete(op.response);
                        op.response.release();
                    }
                    op.request.release();
                    op.release();
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
