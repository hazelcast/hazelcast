/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.util.logging.Level;

public class SocketAcceptor implements Runnable {
    private final ServerSocketChannel serverSocketChannel;
    private final ConnectionManager connectionManager;
    private final ILogger logger;

    public SocketAcceptor(ServerSocketChannel serverSocketChannel, ConnectionManager connectionManager) {
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
    }

    public void run() {
        try {
            connectionManager.ioService.onIOThreadStart();
            serverSocketChannel.configureBlocking(true);
            while (connectionManager.isLive()) {
                SocketChannelWrapper socketChannelWrapper = null;
                try {
                    socketChannelWrapper = connectionManager.wrapSocketChannel(serverSocketChannel.accept(), false);
                } catch (Exception e) {
                    if (e instanceof ClosedChannelException && !connectionManager.isLive()) {
                        // ClosedChannelException
                        // or AsynchronousCloseException
                        // or ClosedByInterruptException
                        logger.log(Level.FINEST, "Terminating socket acceptor thread...", e);
                    } else {
                        logger.log(Level.WARNING, "Unexpected error while accepting connection!", e);
                        try {
                            serverSocketChannel.close();
                        } catch (Exception ignore) {
                        }
                        connectionManager.ioService.onFatalError(e);
                    }
                }
                if (socketChannelWrapper != null) {
                    final SocketChannelWrapper socketChannel = socketChannelWrapper;
                    connectionManager.executeAsync(new Runnable() {
                        public void run() {
                            logger.log(Level.INFO, socketChannel.socket().getLocalPort()
                                    + " is accepting socket connection from "
                                    + socketChannel.socket().getRemoteSocketAddress());
                            try {
                                MemberSocketInterceptor memberSocketInterceptor = connectionManager.getMemberSocketInterceptor();
                                if (memberSocketInterceptor != null) {
                                    memberSocketInterceptor.onAccept(socketChannel.socket());
                                }
                                socketChannel.configureBlocking(false);
                                connectionManager.initSocket(socketChannel.socket());
                                connectionManager.assignSocketChannel(socketChannel);
                            } catch (Exception e) {
                                logger.log(Level.WARNING, e.getMessage(), e);
                                if (socketChannel != null) {
                                    try {
                                        socketChannel.close();
                                    } catch (IOException ignored) {
                                    }
                                }
                            }
                        }
                    });
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
