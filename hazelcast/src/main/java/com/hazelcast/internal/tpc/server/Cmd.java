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
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.util.counters.SwCounter;

import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.FrameCodec.FLAG_RES_CTRL;
import static com.hazelcast.internal.tpc.FrameCodec.RES_CTRL_TYPE_EXCEPTION;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;

/**
 * a {@link Cmd} is comparable with the classic Hazelcast Operation. A Cmd is a task so that it
 * can be scheduled within a TaskGroup.
 * <p>
 * Configure Concurrency at Cmd level? E.g. some operations could require exclusive
 * access while other could allow for concurrent commands.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public abstract class Cmd extends Task {

    public static final int CMD_COMPLETED = 0;
    public static final int CMD_BLOCKED = 1;
    public static final int CMD_EXCEPTION = 2;
    public static final int CMD_YIELD = 3;

    public boolean priority;
    public int partitionId;
    public long callId;
    public ManagerRegistry managerRegistry;
    // The id that uniquely identifies a command.
    public final int id;
    public IOBuffer request;
    public IOBuffer response;
    public CmdAllocator allocator;
    public CmdTaskFactory scheduler;
    public Eventloop eventloop;
    public SwCounter completed;
    public SwCounter exceptions;
    public CmdAllocator cmdAllocator;
    public Consumer<IOBuffer> responseHandler;

    public Cmd(int id) {
        this.id = id;
    }

    /**
     * Is only called once during the whole lifetime of the Cmd.
     */
    public void init() {
    }

    public abstract int runit() throws Exception;

    /**
     * Will be run after every run that doesn't block.
     */
    public void clear() {
    }

    public void release() {
        if (allocator != null) {
            allocator.free(this);
        }
    }

    @Override
    public final int process() {
        if (id == PipelineCmd.ID) {
            throw new UnsupportedOperationException();
            //return runPipeline();
        }

        int runCode;
        Exception exception = null;
        try {
            runCode = runit();
        } catch (Exception e) {
            exception = e;
            runCode = CMD_EXCEPTION;
        }

        switch (runCode) {
            case CMD_COMPLETED:
                completed.inc();
                FrameCodec.setSize(response);
                sendResponse();
                request.release();
                release();
                return TASK_COMPLETED;
            case CMD_YIELD:
                return TASK_YIELD;
            case CMD_BLOCKED:
                return TASK_BLOCKED;
            case CMD_EXCEPTION:
                exception.printStackTrace();
                exceptions.inc();
                response.clear();
                FrameCodec.writeResponseHeader(response, partitionId, callId, FLAG_RES_CTRL);
                response.writeInt(RES_CTRL_TYPE_EXCEPTION);

                // todo: stacktrace is ignored.
                response.writeString(exception.getMessage());
                FrameCodec.setSize(response);

                sendResponse();
                request.release();
                release();
                return TASK_COMPLETED;
            default:
                throw new IllegalStateException("Unknown runCode:" + runCode);
        }
    }

    private void runPipeline() {
        IOBuffer request = this.request;
        IOBuffer response = this.response;
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
            subCmd.partitionId = partitionId;
            subCmd.priority = priority;
            // they share the same callid.
            subCmd.callId = -1;
            subCmd.response = response;
            subCmd.request = request;

            // we need to move to the actual data of the request
            // first int is the size
            // second int is the commandid.
            request.position(subRequestStart + 2 * SIZEOF_INT);

            int runCode;
            Exception exception = null;
            try {
                runCode = subCmd.runit();
            } catch (Exception e) {
                e.printStackTrace();
                exception = e;
                runCode = CMD_EXCEPTION;
            }

            switch (runCode) {
                case CMD_COMPLETED:
                    completed.inc();
                    // we need to set the sub resp size correctly
                    response.putInt(subResponseStart, response.position() - subResponseStart);
                    break;
                case CMD_BLOCKED:
                    throw new UnsupportedOperationException();
                case CMD_EXCEPTION:
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
        sendResponse();
        request.release();
        release();
    }

    private void sendResponse() {
        if (request.socket != null) {
            request.socket.unsafeWriteAndFlush(response);
        } else {
            responseHandler.accept(response);
        }
    }
}
