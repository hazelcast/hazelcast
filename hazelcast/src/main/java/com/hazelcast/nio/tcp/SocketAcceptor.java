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
import java.util.logging.Level;

public class SocketAcceptor implements Runnable {
    private final ServerSocketChannel serverSocketChannel;
    private final TcpIpConnectionManager connectionManager;
    private final ILogger logger;

    public SocketAcceptor(ServerSocketChannel serverSocketChannel, TcpIpConnectionManager connectionManager) {
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
    }

    public void run() {
        Selector selector = null;
        try {
            if (logger.isFinestEnabled()) {
                log(Level.FINEST, "Starting SocketAcceptor on " + serverSocketChannel);
            }
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (connectionManager.isLive()) {
                // block until new connection or interruption.
                final int keyCount = selector.select();
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    continue;
                }
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
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (IOException e) {
            log(Level.SEVERE, e.getClass().getName() + ": " + e.getMessage(), e);
        } finally {
            closeSelector(selector);
        }
    }

    private void closeSelector(Selector selector) {
        if (selector != null) {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Closing selector " + Thread.currentThread().getName());
                }
                selector.close();
            } catch (final Exception e) {
                Logger.getLogger(SocketAcceptor.class).finest("Exception while closing selector", e);
            }
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
                String error = "Unexpected error while accepting connection! "
                        + e.getClass().getName() + ": " + e.getMessage();
                log(Level.WARNING, error);
                try {
                    serverSocketChannel.close();
                } catch (Exception ex) {
                    Logger.getLogger(SocketAcceptor.class).finest("Closing server socket failed", ex);
                }
                connectionManager.ioService.onFatalError(e);
            }
        }
        if (socketChannelWrapper != null) {
            final SocketChannelWrapper socketChannel = socketChannelWrapper;
            log(Level.INFO, "Accepting socket connection from " + socketChannel.socket().getRemoteSocketAddress());
            connectionManager.ioService.executeAsync(new Runnable() {
                public void run() {
                    configureAndAssignSocket(socketChannel);
                }
            });
        }
    }

    private void configureAndAssignSocket(SocketChannelWrapper socketChannel) {
        try {
            connectionManager.initSocket(socketChannel.socket());
            connectionManager.interceptSocket(socketChannel.socket(), true);
            socketChannel.configureBlocking(false);
            connectionManager.assignSocketChannel(socketChannel);
        } catch (Exception e) {
            log(Level.WARNING, e.getClass().getName() + ": " + e.getMessage(), e);
            IOUtil.closeResource(socketChannel);
        }
    }

    private void log(Level level, String message) {
        log(level, message, null);
    }

    private void log(Level level, String message, Exception e) {
        logger.log(level, message, e);
        connectionManager.ioService.getSystemLogService().logConnection(message);
    }

}
