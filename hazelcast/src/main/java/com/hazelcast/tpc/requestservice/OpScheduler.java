package com.hazelcast.tpc.requestservice;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.tpc.util.CircularQueue;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Scheduler;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.tpc.engine.frame.Frame.*;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_PAYLOAD;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_RES_PAYLOAD;
import static com.hazelcast.tpc.requestservice.Op.BLOCKED;
import static com.hazelcast.tpc.requestservice.Op.COMPLETED;
import static com.hazelcast.tpc.requestservice.Op.EXCEPTION;

/**
 * todo: add control on number of requests of single socket.
 * overload can happen at 2 levels
 * 1) a single asyncsocket exceeding the maximum number of requests
 * 2) a single asyncsocket exceeded the maximum number of request of the event loop
 *
 * In the first case we want to slow down the reader and signal sender.
 * In the second case want to slow down all the readers and signal all senders.
 *
 * The problem with slowing down senders is that sockets can send
 * either requests or responses. We want to slow down the rate of sending
 * requests, but we don't want to slow down the rate of responses or
 * other data.
 */
public final class OpScheduler implements Scheduler {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter ticks = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final SwCounter exceptions = newSwCounter();
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

        if (runQueue.offer(op)) {
            runSingle();
        } else {
            Frame response = op.response;
            response.writeResponseHeader(op.partitionId(), op.callId(), FLAG_OP_RESPONSE_CONTROL)
                    .writeInt(RESPONSE_TYPE_OVERLOAD)
                    .writeComplete();
            sendResponse(op);
        }
    }

    @Override
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
            int runCode;
            Exception exception = null;
            try {
                runCode = op.run();
            } catch (Exception e) {
                exception = e;
                runCode = EXCEPTION;
            }

            switch (runCode) {
                case COMPLETED:
                    completed.inc();
                    sendResponse(op);
                    break;
                case BLOCKED:
                    break;
                case EXCEPTION:
                    exceptions.inc();
                    op.response.clear();
                    op.response.writeResponseHeader(op.partitionId(), op.callId(), FLAG_OP_RESPONSE_CONTROL)
                            .writeInt(RESPONSE_TYPE_EXCEPTION)
                            .writeString(exception.getMessage())
                            .writeComplete();
                    sendResponse(op);
                    break;
                default:
                    throw new RuntimeException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return !runQueue.isEmpty();
    }

    private void sendResponse(Op op) {
        if (op.request.future == null) {
            op.request.socket.unsafeWriteAndFlush(op.response);
        } else {
            op.request.future.complete(op.response);
        }
        op.request.release();
        op.release();
    }
}
