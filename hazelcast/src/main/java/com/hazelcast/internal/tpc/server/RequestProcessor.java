/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Processor;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.util.counters.SwCounter;

import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.FrameCodec.FLAG_PRIORITY;
import static com.hazelcast.internal.tpc.FrameCodec.FLAG_RES_CTRL;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_FLAGS;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_PARTITION_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_CALL_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_CMD_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_PAYLOAD;
import static com.hazelcast.internal.tpc.FrameCodec.RES_CTRL_TYPE_EXCEPTION;
import static com.hazelcast.internal.tpc.FrameCodec.RES_CTRL_TYPE_OVERLOAD;
import static com.hazelcast.internal.tpc.server.Cmd.BLOCKED;
import static com.hazelcast.internal.tpc.server.Cmd.COMPLETED;
import static com.hazelcast.internal.tpc.server.Cmd.EXCEPTION;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
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
@SuppressWarnings("checkstyle:MagicNumber")
public final class RequestProcessor implements Processor {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter ticks = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final SwCounter exceptions = newSwCounter();
    private final IOBufferAllocator localResponseAllocator;
    private final IOBufferAllocator remoteResponseAllocator;
    private final CmdAllocator cmdAllocator;
    private Eventloop eventloop;
    private Consumer<IOBuffer> responseHandler;

    public RequestProcessor(CmdRegistry cmdRegistry,
                            ManagerRegistry managerRegistry,
                            IOBufferAllocator localResponseAllocator,
                            IOBufferAllocator remoteResponseAllocator,
                            Consumer<IOBuffer> responseHandler) {
        this.localResponseAllocator = localResponseAllocator;
        this.remoteResponseAllocator = remoteResponseAllocator;
        this.responseHandler = responseHandler;
        this.cmdAllocator = new CmdAllocator(cmdRegistry, 16384) {
            @Override
            public void init(Cmd cmd) {
                cmd.scheduler = RequestProcessor.this;
                cmd.eventloop = RequestProcessor.this.getEventloop();
                cmd.managerRegistry = managerRegistry;
                super.init(cmd);
            }
        };
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
    public void schedule(IOBuffer request) {
        Cmd cmd = cmdAllocator.allocate(request.getInt(OFFSET_REQ_CMD_ID));
        cmd.priority = (request.getInt(OFFSET_FLAGS) & FLAG_PRIORITY) != 0;
        cmd.partitionId = request.getInt(OFFSET_PARTITION_ID);
        cmd.callId = request.getLong(OFFSET_REQ_CALL_ID);
        cmd.response = request.socket != null
                ? remoteResponseAllocator.allocate()
                : localResponseAllocator.allocate();
        cmd.request = request;
        FrameCodec.writeResponseHeader(cmd.response, cmd.partitionId, cmd.callId);
        cmd.request.position(OFFSET_REQ_PAYLOAD);
        schedule(cmd);
    }

    @Override
    public void schedule(Object r) {
        IOBuffer request = (IOBuffer) r;
        schedule(request);
    }

    public void schedule(Cmd cmd) {
        scheduled.inc();

        if (runQueue.offer(cmd)) {
            tryRunOneCmd();
        } else {
            IOBuffer response = cmd.response;
            FrameCodec.writeResponseHeader(response, cmd.partitionId, cmd.callId, FLAG_RES_CTRL);
            response.writeInt(RES_CTRL_TYPE_OVERLOAD);
            FrameCodec.setSize(response);
            sendResponse(cmd);
            cmd.request.release();
            cmd.release();
        }
    }

    @Override
    public boolean tick() {
        ticks.inc();

        for (int k = 0; k < batchSize - 1; k++) {
            if (!tryRunOneCmd()) {
                return false;
            }
        }

        return tryRunOneCmd();
    }

    public boolean tryRunOneCmd() {
        Cmd cmd = priorityRunQueue.poll();
        if (cmd == null) {
            cmd = runQueue.poll();
            if (cmd == null) {
                return false;
            }
        }

        try {
            if (cmd.id == PipelineCmd.ID) {
                runPipeline((PipelineCmd) cmd);
            } else {
                int runCode;
                Exception exception = null;
                try {
                    runCode = cmd.run();
                } catch (Exception e) {
                    exception = e;
                    runCode = EXCEPTION;
                }

                switch (runCode) {
                    case COMPLETED:
                        completed.inc();
                        FrameCodec.setSize(cmd.response);
                        sendResponse(cmd);
                        cmd.request.release();
                        cmd.release();
                        break;
                    case BLOCKED:
                        break;
                    case EXCEPTION:
                        exception.printStackTrace();
                        exceptions.inc();
                        cmd.response.clear();
                        FrameCodec.writeResponseHeader(cmd.response, cmd.partitionId, cmd.callId, FLAG_RES_CTRL);
                        cmd.response.writeInt(RES_CTRL_TYPE_EXCEPTION);

                        // todo: stacktrace is ignored.
                        cmd.response.writeString(exception.getMessage());
                        FrameCodec.setSize(cmd.response);

                        sendResponse(cmd);
                        cmd.request.release();
                        cmd.release();
                        break;
                    default:
                        throw new IllegalStateException("Unknown runCode:" + runCode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return runQueue.hasRemaining() || priorityRunQueue.hasRemaining();
    }

    private void runPipeline(PipelineCmd cmd) {
        IOBuffer request = cmd.request;
        IOBuffer response = cmd.response;
        int count = request.readInt();
        //System.out.println("count:" + count);
        // for now we process the items in the batch, but we should interleave with other requests.
        for (int k = 0; k < count; k++) {
            int subRequestStart = request.position();
            int subResponseStart = response.position();

            // placeholder for the size.
            response.writeInt(0);

            int subReqSize = request.readInt();
            int subCmdId = request.readInt();
            // todo: this should not be repeated.
            Cmd subCmd = cmdAllocator.allocate(subCmdId);
            //System.out.println(subCmdId);
            // they share same partitionId.
            subCmd.partitionId = cmd.partitionId;
            subCmd.priority = cmd.priority;
            // they share the same callid.
            subCmd.callId = -1;
            subCmd.response = cmd.response;
            subCmd.request = request;

            // we need to move to the actual data of the request
            // first int is the size
            // second int is the commandid.
            request.position(subRequestStart + 2 * SIZEOF_INT);

            int runCode;
            Exception exception = null;
            try {
                runCode = subCmd.run();
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

            subCmd.release();
        }

        FrameCodec.setSize(response);
        sendResponse(cmd);
        cmd.request.release();
        cmd.release();
    }

    private void sendResponse(Cmd cmd) {
        if (cmd.request.socket != null) {
            cmd.request.socket.unsafeWriteAndFlush(cmd.response);
        } else {
            responseHandler.accept(cmd.response);
        }
    }
}

