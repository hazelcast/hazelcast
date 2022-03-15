/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.IOUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;

final class ServerSocketHelper {

    private static final int SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int SOCKET_BACKLOG_LENGTH = 100;

    private ServerSocketHelper() {
    }

    /**
     * Creates and binds {@code ServerSocketChannel} using {@code bindAddress} and {@code initialPort}.
     * <p>
     * If the {@code initialPort} is in use, then an available port can be selected by doing an incremental port
     * search if {@link NetworkConfig#isPortAutoIncrement()} allows.
     * <p>
     * When both {@code initialPort} and {@link NetworkConfig#getPort()} are zero, then an ephemeral port will be used.
     * <p>
     * When {@code bindAny} is {@code false}, {@code ServerSocket} will be bound to specific {@code bindAddress}.
     * Otherwise, it will be bound to any local address ({@code 0.0.0.0}).
     *
     * @param logger                logger instance
     * @param endpointConfig        the {@link EndpointConfig} that supplies network configuration for the
     *                              server socket
     * @param bindAddress           InetAddress to bind created {@code ServerSocket}
     * @param port                  initial port number to attempt to bind
     * @param portCount             count of subsequent ports to attempt to bind to if initial port is already bound
     * @param isPortAutoIncrement   when {@code true} attempt to bind to {@code portCount} subsequent ports
     *                              after {@code port} is found already bound
     * @param isReuseAddress        sets reuse address socket option
     * @param bindAny               when {@code true} bind any local address otherwise bind given {@code bindAddress}
     * @return actual port number that created {@code ServerSocketChannel} is bound to
     */
    static ServerSocketChannel createServerSocketChannel(ILogger logger, EndpointConfig endpointConfig, InetAddress bindAddress,
                                                         int port, int portCount, boolean isPortAutoIncrement,
                                                         boolean isReuseAddress, boolean bindAny) {
        logger.finest("inet reuseAddress:" + isReuseAddress);

        if (port == 0) {
            logger.info("No explicit port is given, system will pick up an ephemeral port.");
        }
        int portTrialCount = port > 0 && isPortAutoIncrement ? portCount : 1;

        try {
            return tryOpenServerSocketChannel(endpointConfig, bindAddress, port, isReuseAddress, portTrialCount, bindAny, logger);
        } catch (IOException e) {
            String message = "Cannot bind to a given address: " + bindAddress + ". Hazelcast cannot start. ";
            if (isPortAutoIncrement) {
                message += "Config-port: " + port + ", latest-port: " + (port + portTrialCount - 1);
            } else {
                message += "Port [" + port + "] is already in use and auto-increment is disabled.";
            }
            throw new HazelcastException(message, e);
        }
    }

    private static ServerSocketChannel tryOpenServerSocketChannel(EndpointConfig endpointConfig, InetAddress bindAddress,
                                                          int initialPort, boolean isReuseAddress,  int portTrialCount,
                                                          boolean bindAny, ILogger logger)
            throws IOException {
        assert portTrialCount > 0 : "Port trial count must be positive: " + portTrialCount;

        IOException error = null;
        for (int i = 0; i < portTrialCount; i++) {
            int actualPort = initialPort + i;
            InetSocketAddress socketBindAddress = bindAny
                    ? new InetSocketAddress(actualPort)
                    : new InetSocketAddress(bindAddress, actualPort);
            try {
                return openServerSocketChannel(endpointConfig, socketBindAddress, isReuseAddress, logger);
            } catch (IOException e) {
                error = e;
                if (logger.isFinestEnabled()) {
                    logger.finest("Cannot bind socket address " + socketBindAddress, e);
                }
            }
        }
        throw error;
    }

    private static ServerSocketChannel openServerSocketChannel(EndpointConfig endpointConfig, InetSocketAddress socketBindAddress,
                                                        boolean reuseAddress, ILogger logger)
            throws IOException {

        ServerSocket serverSocket = null;
        ServerSocketChannel serverSocketChannel = null;
        try {
            /*
             * Instead of reusing the ServerSocket/ServerSocketChannel, we are going to close and replace them on
             * every attempt to find a free port. The reason to do this is because in some cases, when concurrent
             * threads/processes try to acquire the same port, the ServerSocket gets corrupted and isn't able to
             * find any free port at all (no matter if there are more than enough free ports available). We have
             * seen this happening on Linux and Windows environments.
             */
            serverSocketChannel = ServerSocketChannel.open();
            serverSocket = serverSocketChannel.socket();
            serverSocket.setReuseAddress(reuseAddress);
            serverSocket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);

            if (endpointConfig != null) {
                serverSocket.setReceiveBufferSize(endpointConfig.getSocketRcvBufferSizeKb() * KILO_BYTE);
            }

            logger.fine("Trying to bind inet socket address: " + socketBindAddress);
            serverSocket.bind(socketBindAddress, SOCKET_BACKLOG_LENGTH);
            logger.fine("Bind successful to inet socket address: " + serverSocket.getLocalSocketAddress());

            serverSocketChannel.configureBlocking(false);
            return serverSocketChannel;
        } catch (IOException e) {
            IOUtil.close(serverSocket);
            IOUtil.closeResource(serverSocketChannel);
            throw e;
        }
    }


}
