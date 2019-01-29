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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ErrorCodec;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.MutableInteger;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.BlockingQueue;

import static com.hazelcast.client.spi.properties.ClientProperty.RESPONSE_THREAD_COUNT;
import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.onOutOfMemory;
import static com.hazelcast.spi.impl.operationservice.impl.InboundResponseHandlerSupplier.getIdleStrategy;
import static com.hazelcast.util.HashUtil.hashToIndex;

/**
 * A {@link Supplier} for {@link ClientResponseHandler} instance.
 *
 * Depending on the configuration the supplier provides:
 * <ol>
 * <li>a on thread ClientResponseHandler (so no offloading to a different thread)</li>
 * <li>a single threaded ClientResponseHandler that offloads the response processing
 * a ResponseThread/li>
 * <li>a multi threaded ClientResponseHandler that offloads the response processing
 * to a pool of ResponseThreads.</li>
 * </ol>
 *
 * {@see InboundResponseHandlerSupplier}.
 */
public class ClientResponseHandlerSupplier implements Supplier<ClientResponseHandler> {

    private static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.client.responsequeue.idlestrategy", "block");

    private static final ThreadLocal<MutableInteger> INT_HOLDER = new ThreadLocal<MutableInteger>() {
        @Override
        protected MutableInteger initialValue() {
            return new MutableInteger();
        }
    };

    private final AbstractClientInvocationService invocationService;
    private final ResponseThread[] responseThreads;
    private final HazelcastClientInstanceImpl client;

    private final ILogger logger;
    private final ClientResponseHandler responseHandler;

    public ClientResponseHandlerSupplier(AbstractClientInvocationService invocationService) {
        this.invocationService = invocationService;
        this.client = invocationService.client;
        this.logger = invocationService.invocationLogger;

        int responseThreadCount = client.getProperties().getInteger(RESPONSE_THREAD_COUNT);
        if (responseThreadCount < 0) {
            throw new IllegalArgumentException(RESPONSE_THREAD_COUNT.getName() + " can't be smaller than 0");
        }
        logger.info("Running with " + responseThreadCount + " response threads");
        this.responseThreads = new ResponseThread[responseThreadCount];
        for (int k = 0; k < responseThreads.length; k++) {
            responseThreads[k] = new ResponseThread(invocationService.client.getName() + ".responsethread-" + k + "-");
        }

        switch (responseThreads.length) {
            case 0:
                this.responseHandler = new SyncResponseHandler();
                break;
            case 1:
                this.responseHandler = new AsyncSingleThreadedResponseHandler();
                break;
            default:
                this.responseHandler = new AsyncMultiThreadedResponseHandler();
        }
    }

    public void start() {
        for (ResponseThread responseThread : responseThreads) {
            responseThread.start();
        }
    }

    public void shutdown() {
        for (ResponseThread responseThread : responseThreads) {
            responseThread.interrupt();
        }
    }

    @Override
    public ClientResponseHandler get() {
        return responseHandler;
    }

    private void process(ClientConnection connection, ClientMessage message) {
        try {
            handleClientMessage(message);
        } catch (Exception e) {
            logger.severe("Failed to process task: " + new ClientPacket(connection, message)
                    + " on responseThread: " + Thread.currentThread().getName(), e);
        } finally {
            connection.decrementPendingPacketCount();
        }
    }

    private void handleClientMessage(ClientMessage clientMessage) {
        long correlationId = clientMessage.getCorrelationId();

        ClientInvocation future = invocationService.deRegisterCallId(correlationId);
        if (future == null) {
            logger.warning("No call for callId: " + correlationId + ", response: " + clientMessage);
            return;
        }

        if (ErrorCodec.TYPE == clientMessage.getMessageType()) {
            future.notifyException(client.getClientExceptionFactory().createException(clientMessage));
        } else {
            future.notify(clientMessage);
        }
    }

    private class ResponseThread extends Thread {
        private final BlockingQueue<ClientPacket> responseQueue;

        ResponseThread(String name) {
            super(name);
            setContextClassLoader(client.getClientConfig().getClassLoader());
            this.responseQueue = new MPSCQueue<ClientPacket>(this, getIdleStrategy(client.getProperties(), IDLE_STRATEGY));
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (OutOfMemoryError e) {
                onOutOfMemory(e);
            } catch (Throwable t) {
                invocationService.invocationLogger.severe(t);
            }
        }

        private void doRun() {
            while (!invocationService.isShutdown()) {
                ClientPacket task;
                try {
                    task = responseQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                process(task.connection, task.message);
            }
        }
    }

    class SyncResponseHandler implements ClientResponseHandler {
        @Override
        public void handle(ClientMessage message, ClientConnection connection) {
            process(connection, message);
        }
    }

    class AsyncSingleThreadedResponseHandler implements ClientResponseHandler {
        @Override
        public void handle(ClientMessage message, ClientConnection connection) {
            responseThreads[0].responseQueue.add(new ClientPacket(connection, message));
        }
    }

    class AsyncMultiThreadedResponseHandler implements ClientResponseHandler {
        @Override
        public void handle(ClientMessage message, ClientConnection connection) {
            int threadIndex = hashToIndex(INT_HOLDER.get().getAndInc(), responseThreads.length);
            responseThreads[threadIndex].responseQueue.add(new ClientPacket(connection, message));
        }
    }

    // https://github.com/hazelcast/hazelcast/issues/12632
    private static class ClientPacket {

        private final ClientConnection connection;
        private final ClientMessage message;

        ClientPacket(ClientConnection connection, ClientMessage message) {
            this.connection = connection;
            this.message = message;
        }
    }
}
