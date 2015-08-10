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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class SocketAcceptorThread extends Thread {
    private static final int SHUTDOWN_TIMEOUT_MILLIS = 1000 * 10;

    private final ServerSocketChannel serverSocketChannel;
    private final TcpIpConnectionManager connectionManager;
    private final ILogger logger;
    private final IOService ioService;

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

    @Override
    public void run() {
        if (logger.isFinestEnabled()) {
            logger.finest("Starting SocketAcceptor on " + serverSocketChannel);
        }

        Selector selector = null;
        try {
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            acceptLoop(selector);
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (IOException e) {
            logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
        } finally {
            closeSelector(selector);
        }
    }

    private void acceptLoop(Selector selector) throws IOException {
        while (connectionManager.isLive()) {
            // block until new connection or interruption.
            int keyCount = selector.select();
            if (isInterrupted()) {
                break;
            }
            if (keyCount == 0) {
                continue;
            }
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey sk = it.next();
                it.remove();
                // of course it is acceptable!
                if (sk.isValid() && sk.isAcceptable()) {
                    acceptSocket();
                }
            }
        }
    }

    private void closeSelector(Selector selector) {
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
        if (!connectionManager.isLive()) {
            return;
        }

        SocketChannelWrapper socketChannelWrapper = null;
        try {
            final SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, false);
            }
        } catch (Exception e) {
            if (e instanceof ClosedChannelException && !connectionManager.isLive()) {
                // ClosedChannelException
                // or AsynchronousCloseException
                // or ClosedByInterruptException
                logger.finest("Terminating socket acceptor thread...", e);
            } else {
                logger.warning("Unexpected error while accepting connection! "
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
            socketChannel.configureBlocking(connectionManager.getThreadingModel().isBlocking());
            connectionManager.newConnection(socketChannel, null);
        } catch (Exception e) {
            logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
            IOUtil.closeResource(socketChannel);
        }
    }

    public void shutdown() {
        logger.finest("Shutting down SocketAcceptor thread.");
        interrupt();
        try {
            join(SHUTDOWN_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
    }
}
