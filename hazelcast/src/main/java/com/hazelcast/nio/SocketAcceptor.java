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

package com.hazelcast.nio;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.*;
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
                final int keyCount = selector.select(); // block until new connection or interruption.
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
                    if (sk.isValid() && sk.isAcceptable()) {  // of course it is acceptable!
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
                    logger.finest( "Closing selector " + Thread.currentThread().getName());
                }
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    private void acceptSocket() {
        if (!connectionManager.isLive()) return;
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
                logger.finest( "Terminating socket acceptor thread...", e);
            } else {
                String error = "Unexpected error while accepting connection! "
                               + e.getClass().getName() + ": " + e.getMessage();
                log(Level.WARNING, error);
                try {
                    serverSocketChannel.close();
                } catch (Exception ignore) {
                }
                connectionManager.ioService.onFatalError(e);
            }
        }
        if (socketChannelWrapper != null) {
            final SocketChannelWrapper socketChannel = socketChannelWrapper;
            log(Level.INFO, "Accepting socket connection from " + socketChannel.socket().getRemoteSocketAddress());
            final MemberSocketInterceptor memberSocketInterceptor = connectionManager.getMemberSocketInterceptor();
            if (memberSocketInterceptor == null) {
                configureAndAssignSocket(socketChannel, null);
            } else {
                connectionManager.ioService.executeAsync(new Runnable() {
                    public void run() {
                        configureAndAssignSocket(socketChannel, memberSocketInterceptor);
                    }
                });
            }
        }
    }

    private void configureAndAssignSocket(SocketChannelWrapper socketChannel, MemberSocketInterceptor memberSocketInterceptor) {
        try {
            connectionManager.initSocket(socketChannel.socket());
            if (memberSocketInterceptor != null) {
                log(Level.FINEST, "Calling member socket interceptor: " + memberSocketInterceptor + " for " + socketChannel);
                memberSocketInterceptor.onAccept(socketChannel.socket());
            }
            socketChannel.configureBlocking(false);
            connectionManager.assignSocketChannel(socketChannel, null);
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
