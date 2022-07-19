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

package com.hazelcast.tpc.requestservice;

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.tpc.requestservice.FrameCodec.FLAG_OP_RESPONSE_CONTROL;
import static com.hazelcast.tpc.requestservice.FrameCodec.OFFSET_RES_CALL_ID;
import static com.hazelcast.tpc.requestservice.FrameCodec.OFFSET_RES_PAYLOAD;
import static com.hazelcast.tpc.requestservice.FrameCodec.RESPONSE_TYPE_EXCEPTION;
import static com.hazelcast.tpc.requestservice.FrameCodec.RESPONSE_TYPE_OVERLOAD;

class ResponseHandler implements Consumer<IOBuffer> {

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
            threads[k] = new ResponseThread(k);
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
    public void accept(IOBuffer response) {
        try {
            if (response.next != null && threadCount > 0) {
                int index = hashToIndex(response.getLong(OFFSET_RES_CALL_ID), threadCount);
                threads[index].queue.add(response);
            } else {
                process(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void process(IOBuffer response) {
        int partitionId = response.getInt(FrameCodec.OFFSET_PARTITION_ID);

        Requests requests;
        if (partitionId >= 0) {
            requests = requestRegistry.getByPartitionId(partitionId);
        } else {
            requests = requestRegistry.getByAddress(response.socket.remoteAddress());
            if (requests == null) {
                System.out.println("Dropping response " + response + ", requests not found");
                response.release();
                return;
            }
        }

        long callId = response.getLong(OFFSET_RES_CALL_ID);

        IOBuffer request = requests.map.remove(callId);
        if (request == null) {
            System.out.println("Dropping response " + response + ", invocation with id " + callId
                    + ", partitionId: " + partitionId + " not found");
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

        private final MPSCQueue<IOBuffer> queue;
        private volatile boolean shuttingdown = false;

        private ResponseThread(int index) {
            super("ResponseThread-" + index);
            this.queue = new MPSCQueue<>(this, null);
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    IOBuffer buf;
                    if (spin) {
                        do {
                            buf = queue.poll();
                        } while (buf == null);
                    } else {
                        buf = queue.take();
                    }

                    do {
                        IOBuffer next = buf.next;
                        buf.next = null;
                        process(buf);
                        buf = next;
                    } while (buf != null);
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
