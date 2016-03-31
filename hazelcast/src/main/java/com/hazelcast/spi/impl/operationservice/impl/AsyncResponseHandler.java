/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * The AsyncResponsePacketHandler is a PacketHandler that asynchronously process operation-response packets.
 *
 * So when a response is received from a remote system, it is put in the responseQueue of the ResponseThread.
 * Then the ResponseThread takes it from this responseQueue and calls the {@link PacketHandler} for the
 * actual processing.
 *
 * The reason that the IO thread doesn't immediately deals with the response is that deserializing the
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} and let the invocation-future
 * deal with the response can be rather expensive currently.
 */
public class AsyncResponseHandler implements PacketHandler, MetricsProvider {

    private final ResponseThread responseThread;
    private final ILogger logger;

    public AsyncResponseHandler(HazelcastThreadGroup threadGroup, ILogger logger, PacketHandler responsePacketHandler) {
        this.logger = logger;
        this.responseThread = new ResponseThread(threadGroup, responsePacketHandler);
    }

    @Probe(name = "responseQueueSize", level = MANDATORY)
    public int getQueueSize() {
        return responseThread.responseQueue.size();
    }

    @Override
    public void handle(Packet packet) {
        checkNotNull(packet, "packet can't be null");
        checkTrue(packet.isFlagSet(FLAG_OP), "FLAG_OP should be set");
        checkTrue(packet.isFlagSet(FLAG_RESPONSE), "FLAG_RESPONSE should be set");

        responseThread.responseQueue.add(packet);
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation");
    }

    public void start() {
        responseThread.start();
    }

    public void shutdown() {
        responseThread.shutdown();
    }

    /**
     * The ResponseThread needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    private final class ResponseThread extends Thread implements OperationHostileThread {

        private final BlockingQueue<Packet> responseQueue = new LinkedBlockingQueue<Packet>();
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
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() throws InterruptedException {
            while (!shutdown) {
                Packet response = responseQueue.take();
                try {
                    responsePacketHandler.handle(response);
                } catch (Throwable e) {
                    inspectOutputMemoryError(e);
                    logger.severe("Failed to process response: " + response + " on response thread:" + getName(), e);
                }
            }
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
