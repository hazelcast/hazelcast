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
import com.hazelcast.internal.tpcengine.Task;
import com.hazelcast.internal.tpcengine.TaskFactory;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.util.counters.SwCounter;

import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.FrameCodec.FLAG_PRIORITY;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_FLAGS;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_PARTITION_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_CALL_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_CMD_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_REQ_PAYLOAD;
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
public final class CmdTaskFactory implements TaskFactory {

    private final SwCounter scheduled = newSwCounter();
    private final SwCounter completed = newSwCounter();
    private final SwCounter exceptions = newSwCounter();
    private final IOBufferAllocator localResponseAllocator;
    private final IOBufferAllocator remoteResponseAllocator;
    private final CmdAllocator cmdAllocator;
    private Eventloop eventloop;
    private Consumer<IOBuffer> responseHandler;

    public CmdTaskFactory(CmdRegistry cmdRegistry,
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
                cmd.scheduler = CmdTaskFactory.this;
                cmd.eventloop = CmdTaskFactory.this.getEventloop();
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
    public Task toTask(Object req) {
        IOBuffer request = (IOBuffer) req;
        Cmd cmd = cmdAllocator.allocate(request.getInt(OFFSET_REQ_CMD_ID));
        cmd.priority = (request.getInt(OFFSET_FLAGS) & FLAG_PRIORITY) != 0;
        cmd.partitionId = request.getInt(OFFSET_PARTITION_ID);
        cmd.callId = request.getLong(OFFSET_REQ_CALL_ID);
        cmd.response = request.socket != null
                ? remoteResponseAllocator.allocate()
                : localResponseAllocator.allocate();
        cmd.request = request;
        cmd.exceptions = exceptions;
        cmd.completed = completed;
        cmd.cmdAllocator = cmdAllocator;
        cmd.responseHandler = responseHandler;
        FrameCodec.writeResponseHeader(cmd.response, cmd.partitionId, cmd.callId);
        cmd.request.position(OFFSET_REQ_PAYLOAD);
        return cmd;
    }
//
//    public void schedule(Cmd cmd) {
//        scheduled.inc();
//
//        if (runQueue.offer(cmd)) {
//            tryRunOneCmd();
//        } else {
//            IOBuffer response = cmd.response;
//            FrameCodec.writeResponseHeader(response, cmd.partitionId, cmd.callId, FLAG_RES_CTRL);
//            response.writeInt(RES_CTRL_TYPE_OVERLOAD);
//            FrameCodec.setSize(response);
//            sendResponse(cmd);
//            cmd.request.release();
//            cmd.release();
//        }
//    }
}

