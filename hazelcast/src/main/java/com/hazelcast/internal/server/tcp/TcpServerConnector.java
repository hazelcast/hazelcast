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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.logging.Level;

import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_CLIENT_BIND;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_CLIENT_BIND_ANY;

/**
 * The TcpServerConnector is responsible to make connections by connecting to a remote serverport. Once completed,
 * it will send the protocol and a {@link com.hazelcast.internal.cluster.impl.MemberHandshake}.
 */
class TcpServerConnector {

    private static final int DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS = 3;
    private static final int MILLIS_PER_SECOND = 1000;

    private final TcpServerConnectionManager connectionManager;
    private final ILogger logger;
    private final ServerContext serverContext;
    private final int outboundPortCount;

    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<>();
    private final boolean socketClientBind;
    private final boolean socketClientBindAny;
    private final int planeCount;

    TcpServerConnector(TcpServerConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.serverContext = connectionManager.getServer().getContext();
        this.logger = serverContext.getLoggingService().getLogger(getClass());
        Collection<Integer> ports = serverContext.getOutboundPorts(connectionManager.getEndpointQualifier());
        this.outboundPortCount = ports.size();
        this.outboundPorts.addAll(ports);
        HazelcastProperties properties = serverContext.properties();
        this.socketClientBind = properties.getBoolean(SOCKET_CLIENT_BIND);
        this.socketClientBindAny = properties.getBoolean(SOCKET_CLIENT_BIND_ANY);
        this.planeCount = connectionManager.planeCount;
    }

    Future<Void> asyncConnect(Address address, boolean silent, int planeIndex) {
        serverContext.shouldConnectTo(address);
        return serverContext.submitAsync(new ConnectTask(address, silent, planeIndex));
    }

    private boolean useAnyOutboundPort() {
        return outboundPortCount == 0;
    }

    private int getOutboundPortCount() {
        return outboundPortCount;
    }

    private int acquireOutboundPort() {
        if (useAnyOutboundPort()) {
            return 0;
        }
        synchronized (outboundPorts) {
            final Integer port = outboundPorts.removeFirst();
            outboundPorts.addLast(port);
            return port;
        }
    }

    private final class ConnectTask implements Runnable {
        private final Address remoteAddress;
        private final boolean silent;
        private final int planeIndex;

        ConnectTask(Address remoteAddress, boolean silent, int planeIndex) {
            this.remoteAddress = remoteAddress;
            this.silent = silent;
            this.planeIndex = planeIndex;
        }

        @Override
        public void run() {
            if (!connectionManager.getServer().isLive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest("ConnectionManager is not live, connection attempt to " + remoteAddress + " is cancelled!");
                }
                return;
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Starting to connect to " + remoteAddress);
            }

            try {
                Address thisAddress = serverContext.getThisAddress();
                if (remoteAddress.isIPv4()) {
                    // remote is IPv4; connect...
                    tryToConnect(remoteAddress.getInetSocketAddress(), serverContext.getSocketConnectTimeoutSeconds(
                            connectionManager.getEndpointQualifier()) * MILLIS_PER_SECOND);
                } else if (thisAddress.isIPv6() && thisAddress.getScopeId() != null) {
                    // Both remote and this addresses are IPv6.
                    // This is a local IPv6 address and scope ID is known.
                    // find correct inet6 address for remote and connect...
                    Inet6Address inetAddress = AddressUtil
                            .getInetAddressFor((Inet6Address) remoteAddress.getInetAddress(), thisAddress.getScopeId());
                    tryToConnect(new InetSocketAddress(inetAddress, remoteAddress.getPort()),
                            serverContext.getSocketConnectTimeoutSeconds(
                                    connectionManager.getEndpointQualifier()) * MILLIS_PER_SECOND);
                } else {
                    // remote is IPv6 and this is either IPv4 or a global IPv6.
                    // find possible remote inet6 addresses and try each one to connect...
                    tryConnectToIPv6();
                }
            } catch (Throwable e) {
                logger.finest(e);
                connectionManager.failedConnection(remoteAddress, planeIndex, e, silent);
            }
        }

        private void tryConnectToIPv6() throws Exception {
            Collection<Inet6Address> possibleInetAddresses = AddressUtil
                    .getPossibleInetAddressesFor((Inet6Address) remoteAddress.getInetAddress());
            Level level = silent ? Level.FINEST : Level.INFO;
            //TODO: collection.toString() will likely not produce any useful output!
            if (logger.isLoggable(level)) {
                logger.log(level, "Trying to connect possible IPv6 addresses: " + possibleInetAddresses);
            }
            boolean connected = false;
            Exception error = null;
            int configuredTimeoutMillis =
                    serverContext.getSocketConnectTimeoutSeconds(connectionManager.getEndpointQualifier()) * MILLIS_PER_SECOND;
            int timeoutMillis = configuredTimeoutMillis > 0 && configuredTimeoutMillis < Integer.MAX_VALUE
                    ? configuredTimeoutMillis : DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS * MILLIS_PER_SECOND;
            for (Inet6Address inetAddress : possibleInetAddresses) {
                try {
                    tryToConnect(new InetSocketAddress(inetAddress, remoteAddress.getPort()), timeoutMillis);
                    connected = true;
                    break;
                } catch (Exception e) {
                    error = e;
                }
            }
            if (!connected && error != null) {
                // could not connect any of addresses
                throw error;
            }
        }

        @SuppressWarnings("unchecked")
        private void tryToConnect(InetSocketAddress socketAddress, int timeout) throws Exception {
            SocketChannel socketChannel = SocketChannel.open();

            TcpServerConnection connection = null;
            Channel channel = connectionManager.newChannel(socketChannel, true);
            channel.attributeMap().put(Address.class, remoteAddress);
            try {
                if (socketClientBind) {
                    bindSocket(socketChannel);
                }

                Level level = silent ? Level.FINEST : Level.INFO;
                if (logger.isLoggable(level)) {
                    logger.log(level, "Connecting to " + socketAddress + ", timeout: " + timeout
                            + ", bind-any: " + socketClientBindAny);
                }

                try {
                    channel.connect(socketAddress, timeout);

                    serverContext.interceptSocket(connectionManager.getEndpointQualifier(), socketChannel.socket(), false);

                    connection = connectionManager.newConnection(channel, remoteAddress, false);
                    new SendMemberHandshakeTask(logger, serverContext, connection,
                            remoteAddress, true, planeIndex, planeCount).run();
                } catch (Exception e) {
                    closeConnection(connection, e);
                    closeSocket(socketChannel);
                    logger.log(level, "Could not connect to: " + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                            + "[" + e.getMessage() + "]");
                    throw e;
                }
            } finally {
                connectionManager.removeAcceptedChannel(channel);
            }
        }

        private void bindSocket(SocketChannel socketChannel) throws IOException {
            InetAddress inetAddress = getInetAddress();
            Socket socket = socketChannel.socket();
            if (useAnyOutboundPort()) {
                SocketAddress socketAddress = new InetSocketAddress(inetAddress, 0);
                socket.bind(socketAddress);
            } else {
                IOException ex = null;
                int retryCount = getOutboundPortCount() * 2;
                for (int i = 0; i < retryCount; i++) {
                    int port = acquireOutboundPort();
                    SocketAddress socketAddress = new InetSocketAddress(inetAddress, port);
                    try {
                        socket.bind(socketAddress);
                        return;
                    } catch (IOException e) {
                        ex = e;
                        logger.finest("Could not bind port[ " + port + "]: " + e.getMessage());
                    }
                }
                throw ex;
            }
        }

        private InetAddress getInetAddress() throws UnknownHostException {
            return socketClientBindAny ? null : serverContext.getThisAddress().getInetAddress();
        }

        private void closeConnection(final Connection connection, Throwable t) {
            if (connection != null) {
                connection.close(null, t);
            }
        }

        private void closeSocket(final SocketChannel socketChannel) {
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (final IOException e) {
                    logger.finest("Closing socket channel failed", e);
                }
            }
        }
    }
}
