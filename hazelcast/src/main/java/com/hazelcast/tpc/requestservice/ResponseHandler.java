package com.hazelcast.tpc.requestservice;

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.tpc.engine.frame.Frame;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.tpc.engine.frame.Frame.FLAG_OP_RESPONSE_CONTROL;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_RES_CALL_ID;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_RES_PAYLOAD;
import static com.hazelcast.tpc.engine.frame.Frame.RESPONSE_TYPE_EXCEPTION;
import static com.hazelcast.tpc.engine.frame.Frame.RESPONSE_TYPE_OVERLOAD;

class ResponseHandler implements Consumer<Frame> {

    private final ResponseThread[] threads;
    private final int threadCount;
    private final boolean spin;
    private final RequestRegistry requestRegistry;

    ResponseHandler(int threadCount,
                    boolean spin,
                    RequestRegistry requestRegistry) {
        this.spin = spin;
        this.threadCount = threadCount;
        this.threads = new ResponseThread[threadCount];
        this.requestRegistry = requestRegistry;
        for (int k = 0; k < threadCount; k++) {
            this.threads[k] = new ResponseThread(k);
        }
    }

    void start() {
        for (ResponseThread t : threads) {
            t.start();
        }
    }

    void shutdown() {
        for (ResponseThread t : threads) {
            t.shutdown();
        }
    }

    @Override
    public void accept(Frame response) {
        try {
            if (response.next != null) {
                int index = threadCount == 0
                        ? 0
                        : hashToIndex(response.getLong(OFFSET_RES_CALL_ID), threadCount);
                threads[index].queue.add(response);
            } else {
                process(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void process(Frame response) {
        RequestRegistry.Requests requests = requestRegistry.get(response.socket.getRemoteAddress());
        if (requests == null) {
            System.out.println("Dropping response " + response + ", requests not found");
            response.release();
            return;
        }

        long callId = response.getLong(OFFSET_RES_CALL_ID);
        //System.out.println("response with callId:"+callId +" frame: "+response);

        Frame request = requests.map.remove(callId);
        if (request == null) {
            System.out.println("Dropping response " + response + ", invocation with id " + callId + " not found");
            return;
        }

        response.position(OFFSET_RES_PAYLOAD);
        CompletableFuture future = request.future;
        int flags = request.flags();
        if ((flags & FLAG_OP_RESPONSE_CONTROL) == 0) {
            future.complete(response);
        } else {
            int type = request.readInt();
            switch (type) {
                case RESPONSE_TYPE_OVERLOAD:
                    // we need to find better solution
                    future.completeExceptionally(new RuntimeException("Server is overloaded"));
                    response.release();
                    break;
                case RESPONSE_TYPE_EXCEPTION:
                    future.completeExceptionally(new RuntimeException(response.readString()));
                    response.release();
                    break;
                default:
                    throw new RuntimeException();
            }
        }

        requests.complete();
        request.release();
    }

    private class ResponseThread extends Thread {

        private final MPSCQueue<Frame> queue;
        private volatile boolean shuttingdown = false;

        private ResponseThread(int index) {
            super("ResponseThread-" + index);
            this.queue = new MPSCQueue<>(this, null);
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    Frame frame;
                    if (spin) {
                        do {
                            frame = queue.poll();
                        } while (frame == null);
                    } else {
                        frame = queue.take();
                    }

                    do {
                        Frame next = frame.next;
                        frame.next = null;
                        process(frame);
                        frame = next;
                    } while (frame != null);
                }
            } catch (InterruptedException e) {
                System.out.println("ResponseThread stopping due to interrupt");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void shutdown() {
            shuttingdown = true;
            interrupt();
        }
    }
}
