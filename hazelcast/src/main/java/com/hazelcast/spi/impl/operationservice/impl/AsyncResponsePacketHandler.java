/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.util.MPSCQueue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.nio.Packet.HEADER_OP;
import static com.hazelcast.nio.Packet.HEADER_RESPONSE;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * The AsyncResponsePacketHandler is a PacketHandler that asynchronously process operation-response packets.
 *
 * So when a response is received from a remote system, it is put in the workQueue of the ResponseThread.
 * Then the ResponseThread takes it from this workQueue and calls the {@link PacketHandler} for the
 * actual processing.
 *
 * The reason that the IO thread doesn't immediately deals with the response is that deserializing the
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} and let the invocation-future
 * deal with the response can be rather expensive currently.
 */
public class AsyncResponsePacketHandler implements PacketHandler {

    private final ResponseThread responseThread;
    private final BlockingQueue<Packet> workQueue;
    private final ILogger logger;

    public AsyncResponsePacketHandler(HazelcastThreadGroup threadGroup,
                                      ILogger logger,
                                      PacketHandler responsePacketHandler) {
        this.logger = logger;
        this.responseThread = new ResponseThread(threadGroup, responsePacketHandler);

        if (Boolean.getBoolean("fastqueue")) {
            boolean spin = Boolean.getBoolean("response.spin");
            workQueue = new MPSCQueue<Packet>(responseThread, spin);
            logger.info("Enabled ResponseHandler.fastQueue with spinning-enabled:" + spin);
        } else {
            workQueue = new LinkedBlockingQueue<Packet>();
        }

        responseThread.start();
    }

    public int getQueueSize() {
        return workQueue.size();
    }

    public void shutdown() {
        responseThread.shutdown();
    }

    @Override
    public void handle(Packet packet) {
        checkNotNull(packet, "packet can't be null");
        checkTrue(packet.isHeaderSet(HEADER_OP), "HEADER_OP should be set");
        checkTrue(packet.isHeaderSet(HEADER_RESPONSE), "HEADER_RESPONSE should be set");

        workQueue.add(packet);
    }

    /**
     * The ResponseThread needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    private final class ResponseThread extends Thread implements OperationHostileThread {

        // field is only written by the response-thread itself, but can be read by other threads.
        volatile long processedResponses;

        private final PacketHandler responsePacketHandler;
        private volatile boolean shutdown;

        public ResponseThread(HazelcastThreadGroup threadGroup,
                              PacketHandler responsePacketHandler) {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("response"));
            setContextClassLoader(threadGroup.getClassLoader());
            this.responsePacketHandler = responsePacketHandler;
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() {
            for (; ; ) {
                Packet responsePacket;
                try {
                    responsePacket = workQueue.take();
                } catch (InterruptedException e) {
                    if (shutdown) {
                        return;
                    }
                    continue;
                }

                if (shutdown) {
                    return;
                }

                process(responsePacket);
            }
        }

        @SuppressFBWarnings({"VO_VOLATILE_INCREMENT" })
        private void process(Packet responsePacket) {
            processedResponses++;
            try {
                responsePacketHandler.handle(responsePacket);
            } catch (Throwable e) {
                inspectOutputMemoryError(e);
                logger.severe("Failed to process response: " + responsePacket + " on response thread:" + getName(), e);
            }
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
