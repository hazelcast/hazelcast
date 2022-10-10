
/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.alto;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Scheduler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.util.counters.SwCounter;

import static com.hazelcast.internal.alto.FrameCodec.FLAG_OP_RESPONSE_CONTROL;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_PARTITION_ID;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_PAYLOAD;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_RES_PAYLOAD;
import static com.hazelcast.internal.alto.FrameCodec.RESPONSE_TYPE_EXCEPTION;
import static com.hazelcast.internal.alto.FrameCodec.RESPONSE_TYPE_OVERLOAD;
import static com.hazelcast.internal.alto.Op.BLOCKED;
import static com.hazelcast.internal.alto.Op.COMPLETED;
import static com.hazelcast.internal.alto.Op.EXCEPTION;
import static com.hazelcast.internal.alto.OpCodes.PIPELINE;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * todo: add control on number of requests of single socket.
 * overload can happen at 2 levels
 * 1) a single asyncsocket exceeding the maximum number of requests
 * 2) a single asyncsocket exceeded the maximum number of request of the event loop
 * <p>
 * In the first case we want to slow down the reader and signal sender.
 * In the second case want to slow down all the readers and signal all senders.
 * <p>
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
    private final IOBufferAllocator localResponseAllocator;
    private final IOBufferAllocator remoteResponseAllocator;
    private final OpAllocator opAllocator;
    private Eventloop eventloop;
    private ResponseHandler responseHandler;

    public OpScheduler(int capacity,
                       int batchSize,
                       Managers managers,
                       IOBufferAllocator localResponseAllocator,
                       IOBufferAllocator remoteResponseAllocator,
                       ResponseHandler responseHandler) {
        this.runQueue = new CircularQueue<>(capacity);
        this.batchSize = batchSize;
        this.localResponseAllocator = localResponseAllocator;
        this.remoteResponseAllocator = remoteResponseAllocator;
        this.responseHandler = responseHandler;
        this.opAllocator = new OpAllocator(this, managers);
    }

    public long getScheduled() {
        return scheduled.get();
    }

    @Override
    public void init(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    public Eventloop getEventloop() {
        return eventloop;
    }

    @Override
    public void schedule(Object r) {
        IOBuffer request = (IOBuffer) r;
        Op op = opAllocator.allocate(request.getInt(FrameCodec.OFFSET_REQ_OPCODE));
        op.partitionId = request.getInt(OFFSET_PARTITION_ID);
        op.callId = request.getLong(OFFSET_REQ_CALL_ID);
        op.response = request.socket != null
                ? remoteResponseAllocator.allocate(OFFSET_RES_PAYLOAD)
                : localResponseAllocator.allocate(OFFSET_RES_PAYLOAD);
        op.request = request;
        FrameCodec.writeResponseHeader(op.response, op.partitionId, op.callId);
        op.request.position(OFFSET_REQ_PAYLOAD);
        schedule(op);
    }

    public void schedule(Op op) {
        scheduled.inc();

        if (runQueue.offer(op)) {
            tryRunOneOp();
        } else {
            IOBuffer response = op.response;
            FrameCodec.writeResponseHeader(response, op.partitionId, op.callId, FLAG_OP_RESPONSE_CONTROL);
            response.writeInt(RESPONSE_TYPE_OVERLOAD);
            FrameCodec.setSize(response);
            sendResponse(op);
        }
    }

    @Override
    public boolean tick() {
        ticks.inc();

        for (int k = 0; k < batchSize - 1; k++) {
            if (!tryRunOneOp()) {
                return false;
            }
        }

        return tryRunOneOp();
    }

    public boolean tryRunOneOp() {
        final Op op = runQueue.poll();
        if (op == null) {
            return false;
        }

        try {
            if (op.opcode == PIPELINE) {
                IOBuffer request = op.request;
                IOBuffer response = op.response;
                int count = request.readInt();
                //System.out.println("count:" + count);
                // for now we process the items in the batch, but we should interleave with other requests.
                for (int k = 0; k < count; k++) {
                    int subRequestStart = request.position();
                    int subResponseStart = response.position();

                    // placeholder for the size.
                    response.writeInt(0);//size

                    int subReqSize = request.readInt();
                    int subOpcode = request.readInt();
                    Op subOp = opAllocator.allocate(subOpcode);
                    //System.out.println(subOpcode);
                    // they share same partitionId.
                    subOp.partitionId = op.partitionId;
                    // they share the same callid.
                    subOp.callId = -1;
                    subOp.response = op.response;
                    subOp.request = request;

                    // we need to move to the actual data of the request
                    // first int is the size
                    // second int is the opcode.
                    request.position(subRequestStart + 2 * SIZEOF_INT);

                    int runCode;
                    Exception exception = null;
                    try {
                        runCode = subOp.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                        exception = e;
                        runCode = EXCEPTION;
                    }

                    switch (runCode) {
                        case COMPLETED:
                            completed.inc();
                            // we need to set the sub resp size correctly
                            response.putInt(subResponseStart, response.position() - subResponseStart);
                            break;
                        case BLOCKED:
                            throw new UnsupportedOperationException();
                        case EXCEPTION:
                            exceptions.inc();
                            throw new UnsupportedOperationException();
                        default:
                            throw new IllegalStateException("Unknown runCode:" + runCode);
                    }

                    // move the position to the next request,
                    request.position(subRequestStart + subReqSize);

                    subOp.release();
                }

                FrameCodec.setSize(response);
                sendResponse(op);
            } else {
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
                        FrameCodec.setSize(op.response);
                        sendResponse(op);
                        break;
                    case BLOCKED:
                        break;
                    case EXCEPTION:
                        exceptions.inc();
                        op.response.clear();
                        FrameCodec.writeResponseHeader(op.response, op.partitionId, op.callId, FLAG_OP_RESPONSE_CONTROL);
                        op.response.writeInt(RESPONSE_TYPE_EXCEPTION);
                        op.response.writeString(exception.getMessage());
                        FrameCodec.setSize(op.response);
                        sendResponse(op);
                        break;
                    default:
                        throw new IllegalStateException("Unknown runCode:" + runCode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return !runQueue.isEmpty();
    }

    private void sendResponse(Op op) {
        if (op.request.socket != null) {
            op.request.socket.unsafeWriteAndFlush(op.response);
        } else {
            responseHandler.accept(op.response);
        }
        op.request.release();
        op.release();
    }
}

