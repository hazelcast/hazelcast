package com.hazelcast.tpc.requestservice;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.tpc.engine.CircularQueue;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Scheduler;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_PAYLOAD;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_RES_PAYLOAD;

public final class OpScheduler implements Scheduler {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter ticks = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final CircularQueue<Op> runQueue;
    private final int batchSize;
    private final FrameAllocator localResponseFrameAllocator;
    private final FrameAllocator remoteResponseFrameAllocator;
    private final OpAllocator opAllocator;
    private Eventloop eventloop;

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
    public void setEventloop(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    public Eventloop getEventloop() {
        return eventloop;
    }

    @Override
    public void schedule(Frame request) {
        Op op = opAllocator.allocate(request);
        op.response = request.future != null
                ? remoteResponseFrameAllocator.allocate(OFFSET_RES_PAYLOAD)
                : localResponseFrameAllocator.allocate(OFFSET_RES_PAYLOAD);
        op.request = request.position(OFFSET_REQ_PAYLOAD);
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
                case Op.COMPLETED:
                    completed.inc();
                    if (op.request.future == null) {
                        op.request.asyncSocket.unsafeWriteAndFlush(op.response);
                    } else {
                        op.request.future.complete(op.response);
                    }
                    op.request.release();
                    op.release();
                    break;
                case Op.BLOCKED:
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
