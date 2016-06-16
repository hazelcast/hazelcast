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
import com.hazelcast.internal.util.collection.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.BusySpinIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.BlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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

    public static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.operation.responsequeue.idlestrategy", "block");

    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    final ResponseThread responseThread;
    private final ILogger logger;

    AsyncResponseHandler(HazelcastThreadGroup threadGroup, ILogger logger, PacketHandler responsePacketHandler,
                         HazelcastProperties properties) {
        this.logger = logger;
        this.responseThread = new ResponseThread(threadGroup, responsePacketHandler, properties);
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

    public static IdleStrategy getIdleStrategy(HazelcastProperties properties, HazelcastProperty property) {
        String idleStrategyString = properties.getString(property);
        if ("block".equals(idleStrategyString)) {
            return null;
        } else if ("backoff".equals(idleStrategyString)) {
            return new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
        } else if ("busyspin".equals(idleStrategyString)) {
            return new BusySpinIdleStrategy();
        } else {
            throw new IllegalStateException("Unrecognized " + property.getName() + " value=" + idleStrategyString);
        }
    }

    /**
     * The ResponseThread needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    final class ResponseThread extends Thread implements OperationHostileThread {

        private final BlockingQueue<Packet> responseQueue;
        private final PacketHandler responsePacketHandler;
        private volatile boolean shutdown;

        private ResponseThread(HazelcastThreadGroup threadGroup,
                               PacketHandler responsePacketHandler,
                               HazelcastProperties properties) {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("response"));
            setContextClassLoader(threadGroup.getClassLoader());
            this.responsePacketHandler = responsePacketHandler;
            this.responseQueue = new MPSCQueue<Packet>(this, getIdleStrategy(properties, IDLE_STRATEGY));
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() throws InterruptedException {
            while (!shutdown) {
                Packet response = responseQueue.take();
                try {
                    responsePacketHandler.handle(response);
                } catch (Throwable e) {
                    inspectOutOfMemoryError(e);
                    logger.severe("Failed to process response: " + response + " on:" + getName(), e);
                }
            }
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
