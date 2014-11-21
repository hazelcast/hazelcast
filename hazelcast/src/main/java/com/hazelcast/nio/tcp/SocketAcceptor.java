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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class SocketAcceptor implements Runnable {

    private final ServerSocketChannel serverSocketChannel;
    private final TcpIpConnectionManager connectionManager;
    private final ILogger logger;

    public SocketAcceptor(ServerSocketChannel serverSocketChannel, TcpIpConnectionManager connectionManager) {
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
    }

    @Override
    public void run() {
        Selector selector = null;
        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Starting SocketAcceptor on " + serverSocketChannel);
            }
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (connectionManager.isAlive()) {
                // block until new connection or interruption.
                final int keyCount = selector.select();
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    continue;
                }
                processSelectionKeys(selector);
            }
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (IOException e) {
            logger.severe(e.getClass().getName() + ": " + e.getMessage(), e);
        } finally {
            closeSelector(selector);
        }
    }

    private void processSelectionKeys(Selector selector) {
        final Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
        final Iterator<SelectionKey> it = setSelectedKeys.iterator();
        while (it.hasNext()) {
            final SelectionKey sk = it.next();
            it.remove();
            // of course it is acceptable!
            if (sk.isValid() && sk.isAcceptable()) {
                acceptSocket();
            }
        }
    }

    private void closeSelector(Selector selector) {
        if (selector == null) {
            return;
        }

        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Closing selector " + Thread.currentThread().getName());
            }
            selector.close();
        } catch (Exception e) {
            //TODO: Why are we not making use of the normal logger?
            Logger.getLogger(SocketAcceptor.class).finest("Exception while closing selector", e);
        }
    }

    private void acceptSocket() {
        if (!connectionManager.isAlive()) {
            return;
        }

        SocketChannelWrapper socketChannelWrapper = createSocketChannelWrapper();

        if (socketChannelWrapper != null) {
            final SocketChannelWrapper socketChannel = socketChannelWrapper;
            logger.info("Accepting socket connection from " + socketChannel.socket().getRemoteSocketAddress());
            if (connectionManager.isSocketInterceptorEnabled()) {
                configureAndAssignSocket(socketChannel);
            } else {
                connectionManager.ioService.executeAsync(new Runnable() {
                    public void run() {
                        configureAndAssignSocket(socketChannel);
                    }
                });
            }
        }
    }

    private SocketChannelWrapper createSocketChannelWrapper() {
        SocketChannelWrapper socketChannelWrapper = null;
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, false);
            }
        } catch (Exception e) {
            if (e instanceof ClosedChannelException && !connectionManager.isAlive()) {
                // ClosedChannelException
                // or AsynchronousCloseException
                // or ClosedByInterruptException
                logger.finest("Terminating socket acceptor thread...", e);
            } else {
                String error = "Unexpected error while accepting connection! "
                        + e.getClass().getName() + ": " + e.getMessage();
                logger.warning(error);
                try {
                    serverSocketChannel.close();
                } catch (Exception ex) {
                    //todo: why are we not using the normal logger.
                    Logger.getLogger(SocketAcceptor.class).finest("Closing server socket failed", ex);
                }
                connectionManager.ioService.onFatalError(e);
            }
        }
        return socketChannelWrapper;
    }

    private void configureAndAssignSocket(SocketChannelWrapper socketChannel) {
        try {
            connectionManager.initSocket(socketChannel.socket());
            connectionManager.interceptSocket(socketChannel.socket(), true);
            socketChannel.configureBlocking(false);
            connectionManager.assignSocketChannel(socketChannel, null);
        } catch (Exception e) {
            logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
            IOUtil.closeResource(socketChannel);
        }
    }
}
