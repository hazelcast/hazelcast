/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.MutableInteger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.BusySpinIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.BlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.properties.GroupProperty.RESPONSE_THREAD_COUNT;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static com.hazelcast.util.concurrent.BackoffIdleStrategy.createBackoffIdleStrategy;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link Supplier} responsible for providing a {@link Consumer} that
 * processes inbound responses.
 *
 * Depending on the {@link com.hazelcast.spi.properties.GroupProperty#RESPONSE_THREAD_COUNT}
 * it will return the appropriate response handler:
 * <ol>
 * <li>a 'sync' response handler that doesn't offload to a different thread and
 * processes the response on the calling (IO) thread.</li>
 * <li>a single threaded Packet Consumer that offloads the response processing a
 * ResponseThread/li>
 * <li>a multi threaded Packet Consumer that offloads the response processing
 * to a pool of ResponseThreads.</li>
 * </ol>
 * Having multiple threads processing responses improves performance and
 * stability of the throughput.
 *
 * In case of asynchronous response processing, the response is put in the
 * responseQueue of the ResponseThread. Then the ResponseThread takes it from
 * this responseQueue and calls a {@link Consumer} for the actual processing.
 *
 * The reason that the IO thread doesn't immediately deal with the response is that
 * dealing with the response and especially notifying the invocation future can be
 * very expensive.
 */
public class InboundResponseHandlerSupplier implements MetricsProvider, Supplier<Consumer<Packet>> {

    public static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.operation.responsequeue.idlestrategy", "block");

    private static final ThreadLocal<MutableInteger> INT_HOLDER = new ThreadLocal<MutableInteger>() {
        @Override
        protected MutableInteger initialValue() {
            return new MutableInteger();
        }
    };

    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    private final ResponseThread[] responseThreads;
    private final ILogger logger;
    private final Consumer<Packet> responseHandler;
    // these references are needed for metrics.
    private final InboundResponseHandler[] inboundResponseHandlers;
    private final NodeEngine nodeEngine;
    private final InvocationRegistry invocationRegistry;
    private final HazelcastProperties properties;

    InboundResponseHandlerSupplier(ClassLoader classLoader,
                                   InvocationRegistry invocationRegistry,
                                   String hzName,
                                   NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.invocationRegistry = invocationRegistry;
        this.logger = nodeEngine.getLogger(InboundResponseHandlerSupplier.class);
        this.properties = nodeEngine.getProperties();
        int responseThreadCount = properties.getInteger(RESPONSE_THREAD_COUNT);
        if (responseThreadCount < 0) {
            throw new IllegalArgumentException(RESPONSE_THREAD_COUNT.getName() + " can't be smaller than 0");
        }

        logger.info("Running with " + responseThreadCount + " response threads");

        this.responseThreads = new ResponseThread[responseThreadCount];
        if (responseThreadCount == 0) {
            inboundResponseHandlers = new InboundResponseHandler[1];
            inboundResponseHandlers[0] = new InboundResponseHandler(invocationRegistry, nodeEngine);
            responseHandler = inboundResponseHandlers[0];
        } else {
            inboundResponseHandlers = new InboundResponseHandler[responseThreadCount];
            for (int k = 0; k < responseThreads.length; k++) {
                ResponseThread responseThread = new ResponseThread(hzName, k);
                responseThread.setContextClassLoader(classLoader);
                responseThreads[k] = responseThread;
                inboundResponseHandlers[k] = responseThread.inboundResponseHandler;
            }

            this.responseHandler = responseThreadCount == 1
                    ? new AsyncSingleThreadedResponseHandler()
                    : new AsyncMultithreadedResponseHandler();
        }
    }

    public InboundResponseHandler backupHandler() {
        return inboundResponseHandlers[0];
    }

    @Probe(level = MANDATORY)
    public int responseQueueSize() {
        int result = 0;
        for (ResponseThread responseThread : responseThreads) {
            result += responseThread.responseQueue.size();
        }
        return result;
    }

    @Probe(name = "responses[normal]", level = MANDATORY)
    long responsesNormal() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesNormal.get();
        }
        return result;
    }

    @Probe(name = "responses[timeout]", level = MANDATORY)
    long responsesTimeout() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesTimeout.get();
        }
        return result;
    }

    @Probe(name = "responses[backup]", level = MANDATORY)
    long responsesBackup() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesBackup.get();
        }
        return result;
    }

    @Probe(name = "responses[error]", level = MANDATORY)
    long responsesError() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesError.get();
        }
        return result;
    }

    @Probe(name = "responses[missing]", level = MANDATORY)
    long responsesMissing() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesMissing.get();
        }
        return result;
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "operation");
    }

    @Override
    public Consumer<Packet> get() {
        return responseHandler;
    }

    public void start() {
        for (ResponseThread responseThread : responseThreads) {
            responseThread.start();
        }
    }

    public void shutdown() {
        for (ResponseThread responseThread : responseThreads) {
            responseThread.shutdown();
        }
    }

    public static IdleStrategy getIdleStrategy(HazelcastProperties properties, HazelcastProperty property) {
        String idleStrategyString = properties.getString(property);
        if ("block".equals(idleStrategyString)) {
            return null;
        } else if ("busyspin".equals(idleStrategyString)) {
            return new BusySpinIdleStrategy();
        } else if ("backoff".equals(idleStrategyString)) {
            return new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
        } else if (idleStrategyString.startsWith("backoff,")) {
            return createBackoffIdleStrategy(idleStrategyString);
        } else {
            throw new IllegalStateException("Unrecognized " + property.getName() + " value=" + idleStrategyString);
        }
    }

    final class AsyncSingleThreadedResponseHandler implements Consumer<Packet> {
        private final ResponseThread responseThread;

        private AsyncSingleThreadedResponseHandler() {
            this.responseThread = responseThreads[0];
        }

        @Override
        public void accept(Packet packet) {
            // there is only one thread, no need to do a mod.
            responseThread.responseQueue.add(packet);
        }
    }

    final class AsyncMultithreadedResponseHandler implements Consumer<Packet> {
        @Override
        public void accept(Packet packet) {
            int threadIndex = hashToIndex(INT_HOLDER.get().getAndInc(), responseThreads.length);
            responseThreads[threadIndex].responseQueue.add(packet);
        }
    }

    /**
     * The ResponseThread needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    private final class ResponseThread extends Thread implements OperationHostileThread {

        private final BlockingQueue<Packet> responseQueue;
        private final InboundResponseHandler inboundResponseHandler;
        private volatile boolean shutdown;

        private ResponseThread(String hzName, int threadIndex) {
            super(createThreadName(hzName, "response-" + threadIndex));
            this.inboundResponseHandler = new InboundResponseHandler(invocationRegistry, nodeEngine);
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
                    inboundResponseHandler.accept(response);
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
