/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.ClientResponse;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.executor.CompletableFutureTask;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.util.executor.StripedExecutor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;

public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private static final ILogger LOGGER = Logger.getLogger(ClientExecutionService.class);

    private final SerializationService serializationService;
    private final ExecutorService executor;
    private final ExecutorService internalExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ResponseThread responseThread;
    private final StripedExecutor eventExecutor;
    private volatile boolean isShutdown;

    public ClientExecutionServiceImpl(String name, ThreadGroup threadGroup, ClassLoader classLoader, int poolSize
            , SerializationService serializationService, int eventThreadCount, int eventQueueCapacity) {
        int executorPoolSize = poolSize;
        if (executorPoolSize <= 0) {
            executorPoolSize = Runtime.getRuntime().availableProcessors();
        }

        this.serializationService = serializationService;
        internalExecutor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".internal-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        LOGGER.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });
        executor = new ThreadPoolExecutor(executorPoolSize, executorPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PoolExecutorThreadFactory(threadGroup, name + ".cached-", classLoader),
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
                        LOGGER.finest(message);
                        throw new RejectedExecutionException(message);
                    }
                });

        eventExecutor = new StripedExecutor(LOGGER, name + ".event", threadGroup, eventThreadCount, eventQueueCapacity);

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                new SingleExecutorThreadFactory(threadGroup, classLoader, name + ".scheduled"));

        responseThread = new ResponseThread(threadGroup, name + ".response-", classLoader);
        responseThread.start();
    }

    public <T> ICompletableFuture<T> submitInternal(final Callable<T> command) {
        CompletableFutureTask futureTask = new CompletableFutureTask(command, internalExecutor);
        internalExecutor.submit(futureTask);
        return futureTask;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public ICompletableFuture<?> submit(Runnable task) {
        CompletableFutureTask futureTask = new CompletableFutureTask(task, null, getAsyncExecutor());
        executor.submit(futureTask);
        return futureTask;
    }

    @Override
    public <T> ICompletableFuture<T> submit(Callable<T> task) {
        CompletableFutureTask<T> futureTask = new CompletableFutureTask<T>(task, getAsyncExecutor());
        executor.submit(futureTask);
        return futureTask;
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(new Runnable() {
            public void run() {
                execute(command);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                execute(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                execute(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ExecutorService getAsyncExecutor() {
        return executor;
    }

    public void shutdown() {
        isShutdown = true;
        internalExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        executor.shutdownNow();
    }

    public void execute(Packet packet) {
        if (packet.isHeaderSet(Packet.HEADER_CLIENT_REQUEST)) {
            responseThread.workQueue.add(packet);
        } else if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
            try {
                eventExecutor.execute(new ClientEventProcessor(packet));
            } catch (RejectedExecutionException e) {
                LOGGER.log(Level.WARNING, " event packet could not be handled ", e);
            }
        } else {
            LOGGER.log(Level.WARNING, "client response packet header is invalid. Dropping the packet ");
        }
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<Packet> workQueue = new LinkedBlockingQueue<Packet>();

        public ResponseThread(ThreadGroup threadGroup, String name, ClassLoader classLoader) {
            super(threadGroup, name);
            setContextClassLoader(classLoader);
        }

        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                LOGGER.severe(t);
            }
        }

        private void doRun() {
            for (;;) {
                Packet task;
                try {
                    task = workQueue.take();
                } catch (InterruptedException e) {
                    if (isShutdown) {
                        return;
                    }
                    continue;
                }

                if (isShutdown) {
                    return;
                }
                process(task);
            }
        }

        private void process(Packet packet) {
            try {
                final ClientConnection conn = (ClientConnection) packet.getConn();
                final ClientResponse clientResponse = serializationService.toObject(packet.getData());
                final int callId = clientResponse.getCallId();
                final Data response = clientResponse.getResponse();
                handlePacket(response, clientResponse.isError(), callId, conn);
                conn.decrementPacketCount();
            } catch (Exception e) {
                LOGGER.severe("Failed to process task: " + packet + " on responseThread :" + getName());
            }
        }

        private void handlePacket(Object response, boolean isError, int callId, ClientConnection conn) {
            final ClientCallFuture future = conn.deRegisterCallId(callId);
            if (future == null) {
                LOGGER.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            if (isError) {
                response = serializationService.toObject(response);
            }
            future.notify(response);
        }

    }

    private final class ClientEventProcessor implements Runnable {
        final Packet packet;

        private ClientEventProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = serializationService.toObject(packet.getData());
            final int callId = clientResponse.getCallId();
            final Data response = clientResponse.getResponse();
            handleEvent(response, callId, conn);
        }

        private void handleEvent(Data event, int callId, ClientConnection conn) {
            final EventHandler eventHandler = conn.getEventHandler(callId);
            final Object eventObject = serializationService.toObject(event);
            if (eventHandler == null) {
                LOGGER.warning("No eventHandler for callId: " + callId + ", event: " + eventObject + ", conn: " + conn);
                return;
            }
            eventHandler.handle(eventObject);
        }
    }
}
