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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.nonblocking.SelectorMode;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SocketAcceptorThread extends Thread {
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
    // last time select returned
    private volatile long lastSelectTimeMs;

    // When true, enables workaround for bug occuring when SelectorImpl.select returns immediately
    // with no channels selected, resulting in 100% CPU usage while doing no progress.
    // See issue: https://github.com/hazelcast/hazelcast/issues/7943
    private final boolean selectorWorkaround = (SelectorMode.getConfiguredValue() == SelectorMode.SELECT_WITH_FIX);

    private volatile boolean live = true;
    private volatile Selector selector;
    private SelectionKey selectionKey;

    public SocketAcceptorThread(
            ThreadGroup threadGroup,
            String name,
            ServerSocketChannel serverSocketChannel,
            TcpIpConnectionManager connectionManager) {
        super(threadGroup, name);
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLogger(this.getClass().getName());
    }

    /**
     * A probe that measure how long this {@link SocketAcceptorThread} has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    @Override
    public void run() {
        if (logger.isFinestEnabled()) {
            logger.finest("Starting SocketAcceptor on " + serverSocketChannel);
        }

        try {
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            if (selectorWorkaround) {
                acceptLoopWithSelectorFix();
            } else {
                acceptLoop();
            }
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (IOException e) {
            logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
        } finally {
            closeSelector();
        }
    }

    private void acceptLoop() throws IOException {
        while (live) {
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
        while (live) {
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
        selectionKey = serverSocketChannel.register(newSelector, SelectionKey.OP_ACCEPT);
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
        SocketChannelWrapper socketChannelWrapper = null;
        try {
            final SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, false);
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

        if (socketChannelWrapper != null) {
            final SocketChannelWrapper socketChannel = socketChannelWrapper;
            logger.info("Accepting socket connection from " + socketChannel.socket().getRemoteSocketAddress());
            if (connectionManager.isSocketInterceptorEnabled()) {
                configureAndAssignSocket(socketChannel);
            } else {
                ioService.executeAsync(new Runnable() {
                    @Override
                    public void run() {
                        configureAndAssignSocket(socketChannel);
                    }
                });
            }
        }
    }

    private void configureAndAssignSocket(SocketChannelWrapper socketChannel) {
        try {
            connectionManager.initSocket(socketChannel.socket());
            connectionManager.interceptSocket(socketChannel.socket(), true);
            socketChannel.configureBlocking(connectionManager.getIoThreadingModel().isBlocking());
            connectionManager.newConnection(socketChannel, null);
        } catch (Exception e) {
            exceptionCount.inc();
            logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
            closeResource(socketChannel);
        }
    }

    public synchronized void shutdown() {
        if (!live) {
            return;
        }

        logger.finest("Shutting down SocketAcceptor thread.");
        live = false;
        Selector sel = selector;
        if (sel != null) {
            sel.wakeup();
        }
        try {
            join(SHUTDOWN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
    }
}
