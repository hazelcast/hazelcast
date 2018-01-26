/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.nio.SelectorMode;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_WITH_FIX;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains the logic for accepting TcpIpConnections.
 *
 * The {@link TcpIpAcceptor} and {@link TcpIpConnector} are 2 sides of the same coin. The {@link TcpIpConnector} take care
 * of the 'client' side of a connection and the {@link TcpIpAcceptor} is the 'server' side of a connection (each connection
 * has a client and server-side
 */
public class TcpIpAcceptor implements MetricsProvider {
    private static final long SHUTDOWN_TIMEOUT_MILLIS = SECONDS.toMillis(10);
    private static final long SELECT_TIMEOUT_MILLIS = SECONDS.toMillis(60);
    private static final int SELECT_IDLE_COUNT_THRESHOLD = 10;

    private final ServerSocketChannel serverSocketChannel;
    private final TcpIpConnectionManager connectionManager;
    private final ILogger logger;
    private final IOService ioService;
    @Probe
    private final SwCounter eventCount = newSwCounter();
    @Probe
    private final SwCounter exceptionCount = newSwCounter();
    // count number of times the selector was recreated (if selectWorkaround is enabled)
    @Probe
    private final SwCounter selectorRecreateCount = newSwCounter();
    private final AcceptorIOThread acceptorThread;
    // last time select returned
    private volatile long lastSelectTimeMs;

    // When true, enables workaround for bug occuring when SelectorImpl.select returns immediately
    // with no channels selected, resulting in 100% CPU usage while doing no progress.
    // See issue: https://github.com/hazelcast/hazelcast/issues/7943
    private final boolean selectorWorkaround = (SelectorMode.getConfiguredValue() == SELECT_WITH_FIX);

    private volatile boolean stop;
    private volatile Selector selector;
    private SelectionKey selectionKey;

    public TcpIpAcceptor(ServerSocketChannel serverSocketChannel, TcpIpConnectionManager connectionManager) {
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLoggingService().getLogger(getClass());
        this.acceptorThread = new AcceptorIOThread();
    }

    /**
     * A probe that measure how long this {@link TcpIpAcceptor} has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "tcp." + acceptorThread.getName());
    }

    public TcpIpAcceptor start() {
        acceptorThread.start();
        return this;
    }

    public synchronized void shutdown() {
        if (stop) {
            return;
        }

        logger.finest("Shutting down SocketAcceptor thread.");
        stop = true;
        Selector sel = selector;
        if (sel != null) {
            sel.wakeup();
        }

        try {
            acceptorThread.join(SHUTDOWN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
    }

    private final class AcceptorIOThread extends Thread {

        private AcceptorIOThread() {
            super(createThreadPoolName(ioService.getHazelcastName(), "IO") + "Acceptor");
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Starting TcpIpAcceptor on " + serverSocketChannel);
            }

            try {
                selector = Selector.open();
                serverSocketChannel.configureBlocking(false);
                selectionKey = serverSocketChannel.register(selector, OP_ACCEPT);
                if (selectorWorkaround) {
                    acceptLoopWithSelectorFix();
                } else {
                    acceptLoop();
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable e) {
                logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
            } finally {
                closeSelector();
            }
        }

        private void acceptLoop() throws IOException {
            while (!stop) {
                // block until new connection or interruption.
                int keyCount = selector.select();
                if (isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    continue;
                }
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                handleSelectionKeys(it);
            }
        }

        private void acceptLoopWithSelectorFix() throws IOException {
            int idleCount = 0;
            while (!stop) {
                // block with a timeout until new connection or interruption.
                long before = currentTimeMillis();
                int keyCount = selector.select(SELECT_TIMEOUT_MILLIS);
                if (isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    long selectTimeTaken = currentTimeMillis() - before;
                    idleCount = selectTimeTaken < SELECT_TIMEOUT_MILLIS ? idleCount + 1 : 0;
                    // select unblocked before timing out with no keys selected --> bug detected
                    if (idleCount > SELECT_IDLE_COUNT_THRESHOLD) {
                        // rebuild the selector
                        rebuildSelector();
                        idleCount = 0;
                    }
                    continue;
                }
                idleCount = 0;
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                handleSelectionKeys(it);
            }
        }

        private void rebuildSelector() throws IOException {
            selectorRecreateCount.inc();
            // cancel existing selection key, register new one on the new selector
            selectionKey.cancel();
            closeSelector();
            Selector newSelector = Selector.open();
            selector = newSelector;
            selectionKey = serverSocketChannel.register(newSelector, OP_ACCEPT);
        }

        private void handleSelectionKeys(Iterator<SelectionKey> it) {
            lastSelectTimeMs = currentTimeMillis();
            while (it.hasNext()) {
                SelectionKey sk = it.next();
                it.remove();
                // of course it is acceptable!
                if (sk.isValid() && sk.isAcceptable()) {
                    eventCount.inc();
                    acceptSocket();
                }
            }
        }

        private void closeSelector() {
            if (selector == null) {
                return;
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Closing selector " + Thread.currentThread().getName());
            }

            try {
                selector.close();
            } catch (Exception e) {
                logger.finest("Exception while closing selector", e);
            }
        }

        private void acceptSocket() {
            Channel channel = null;
            try {
                SocketChannel socketChannel = serverSocketChannel.accept();
                if (socketChannel != null) {
                    channel = connectionManager.createChannel(socketChannel, false);
                }
            } catch (Exception e) {
                exceptionCount.inc();

                if (e instanceof ClosedChannelException && !connectionManager.isLive()) {
                    // ClosedChannelException
                    // or AsynchronousCloseException
                    // or ClosedByInterruptException
                    logger.finest("Terminating socket acceptor thread...", e);
                } else {
                    logger.severe("Unexpected error while accepting connection! "
                            + e.getClass().getName() + ": " + e.getMessage());
                    try {
                        serverSocketChannel.close();
                    } catch (Exception ex) {
                        logger.finest("Closing server socket failed", ex);
                    }
                    ioService.onFatalError(e);
                }
            }

            if (channel != null) {
                final Channel theChannel = channel;
                logger.info("Accepting socket connection from " + theChannel.socket().getRemoteSocketAddress());
                if (ioService.isSocketInterceptorEnabled()) {
                    configureAndAssignSocket(theChannel);
                } else {
                    ioService.executeAsync(new Runnable() {
                        @Override
                        public void run() {
                            configureAndAssignSocket(theChannel);
                        }
                    });
                }
            }
        }

        private void configureAndAssignSocket(Channel channel) {
            try {
                ioService.configureSocket(channel.socket());
                ioService.interceptSocket(channel.socket(), true);
                connectionManager.newConnection(channel, null);
            } catch (Exception e) {
                exceptionCount.inc();
                logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
                closeResource(channel);
            }
        }
    }
}
