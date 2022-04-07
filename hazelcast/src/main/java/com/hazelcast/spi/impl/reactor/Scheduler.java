package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.util.counters.SwCounter;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.reactor.Op.COMPLETED;

public final class Scheduler {

    private final SwCounter ticks = newSwCounter();
    private final CircularQueue<Op> runQueue;
    private final int batchSize;

    public Scheduler(int capacity, int batchSize) {
        this.runQueue = new CircularQueue<>(capacity);
        this.batchSize = batchSize;
    }

    public void schedule(Op op) {
        if (!runQueue.enqueue(op)) {
            //todo: return false and send some kind of rejection message
            throw new RuntimeException("Scheduler overloaded");
        }
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

        Op op = runQueue.dequeue();
        if (op == null) {
            return false;
        }

        try {
            int runCode = op.run();
            switch (runCode) {
                case COMPLETED:
                    if (op.request.future == null) {
                        op.request.channel.writeAndFlush(op.response);
                    } else {
                        op.request.future.complete(op.response);
                        op.response.release();
                    }
                    op.request.release();
                    op.request = null;
                    op.response = null;
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
