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

package com.hazelcast.instance;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;

abstract class AbstractAddressPicker implements AddressPicker {
    private static final int SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int SOCKET_BACKLOG_LENGTH = 100;

    final ILogger logger;
    private final NetworkConfig networkConfig;

    private ServerSocketChannel serverSocketChannel;

    AbstractAddressPicker(NetworkConfig networkConfig, ILogger logger) {
        this.networkConfig = networkConfig;
        this.logger = logger;
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
     * @param bindAddress InetAddress to bind created {@code ServerSocket}
     * @param initialPort Initial port number to attempt to bind
     * @param bindAny     Flag to decide whether to bind given {@code bindAddress} or any local address
     * @return actual port number that created {@code ServerSocketChannel} is bound to
     */
    final int createServerSocketChannel(InetAddress bindAddress, int initialPort, boolean bindAny) {
        int portCount = networkConfig.getPortCount();
        boolean portAutoIncrement = networkConfig.isPortAutoIncrement();
        logger.finest("inet reuseAddress:" + networkConfig.isReuseAddress());

        if (initialPort == 0) {
            initialPort = networkConfig.getPort() ;
        }
        if (initialPort == 0) {
            logger.info("No explicit port is given, system will pick up an ephemeral port.");
        }
        int portTrialCount = initialPort > 0 && portAutoIncrement ? portCount : 1;

        try {
            return tryOpenServerSocketChannel(bindAddress, initialPort, portTrialCount, bindAny);
        } catch (IOException e) {
            String message = "Cannot bind to a given address: " + bindAddress + ". Hazelcast cannot start. ";
            if (networkConfig.isPortAutoIncrement()) {
                message += "Config-port: " + networkConfig.getPort() + ", latest-port: " + (initialPort + portTrialCount - 1);
            } else {
                message += "Port [" + initialPort + "] is already in use and auto-increment is disabled.";
            }
            throw new HazelcastException(message, e);
        }
    }

    private int tryOpenServerSocketChannel(InetAddress bindAddress, int initialPort, int portTrialCount, boolean bindAny)
            throws IOException {
        assert portTrialCount > 0 : "Port trial count must be positive: " + portTrialCount;

        IOException error = null;
        for (int i = 0; i < portTrialCount; i++) {
            int actualPort = initialPort + i;
            boolean reuseAddress = networkConfig.isReuseAddress();
            InetSocketAddress socketBindAddress = bindAny
                    ? new InetSocketAddress(actualPort)
                    : new InetSocketAddress(bindAddress, actualPort);
            try {
                return openServerSocketChannel(socketBindAddress, reuseAddress);
            } catch (IOException e) {
                error = e;
            }
        }
        throw error;
    }

    private int openServerSocketChannel(InetSocketAddress socketBindAddress, boolean reuseAddress)
            throws IOException {

        ServerSocket serverSocket = null;
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

            logger.fine("Trying to bind inet socket address: " + socketBindAddress);
            serverSocket.bind(socketBindAddress, SOCKET_BACKLOG_LENGTH);
            logger.fine("Bind successful to inet socket address: " + serverSocket.getLocalSocketAddress());

            serverSocketChannel.configureBlocking(false);
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            IOUtil.close(serverSocket);
            IOUtil.closeResource(serverSocketChannel);
            throw e;
        }
    }

    @Override
    public final ServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
    }
}
