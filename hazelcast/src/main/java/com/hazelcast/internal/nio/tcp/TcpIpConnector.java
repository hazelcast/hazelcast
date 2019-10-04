/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.util.AddressUtil;

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
import java.util.logging.Level;

/**
 * The TcpIpConnector is responsible to make connections by connecting to a remote serverport. Once completed,
 * it will send the protocol and a bind-message.
 */
class TcpIpConnector {

    private static final int DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS = 3;
    private static final int MILLIS_PER_SECOND = 1000;

    private final TcpIpEndpointManager endpointManager;

    private final ILogger logger;
    private final IOService ioService;
    private final int outboundPortCount;

    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();

    TcpIpConnector(TcpIpEndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        this.ioService = endpointManager.getNetworkingService().getIoService();
        this.logger = ioService.getLoggingService().getLogger(getClass());
        Collection<Integer> ports = ioService.getOutboundPorts(endpointManager.getEndpointQualifier());
        this.outboundPortCount = ports.size();
        this.outboundPorts.addAll(ports);
    }

    void asyncConnect(Address address, boolean silent) {
        ioService.shouldConnectTo(address);
        ioService.executeAsync(new ConnectTask(address, silent));
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
        private final Address address;
        private final boolean silent;

        ConnectTask(Address address, boolean silent) {
            this.address = address;
            this.silent = silent;
        }

        @Override
        public void run() {
            if (!endpointManager.getNetworkingService().isLive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest("ConnectionManager is not live, connection attempt to " + address + " is cancelled!");
                }
                return;
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Starting to connect to " + address);
            }

            try {
                Address thisAddress = ioService.getThisAddress();
                if (address.isIPv4()) {
                    // remote is IPv4; connect...
                    tryToConnect(address.getInetSocketAddress(), ioService.getSocketConnectTimeoutSeconds(
                            endpointManager.getEndpointQualifier()) * MILLIS_PER_SECOND);
                } else if (thisAddress.isIPv6() && thisAddress.getScopeId() != null) {
                    // Both remote and this addresses are IPv6.
                    // This is a local IPv6 address and scope ID is known.
                    // find correct inet6 address for remote and connect...
                    Inet6Address inetAddress = AddressUtil
                            .getInetAddressFor((Inet6Address) address.getInetAddress(), thisAddress.getScopeId());
                    tryToConnect(new InetSocketAddress(inetAddress, address.getPort()),
                            ioService.getSocketConnectTimeoutSeconds(
                                    endpointManager.getEndpointQualifier()) * MILLIS_PER_SECOND);
                } else {
                    // remote is IPv6 and this is either IPv4 or a global IPv6.
                    // find possible remote inet6 addresses and try each one to connect...
                    tryConnectToIPv6();
                }
            } catch (Throwable e) {
                logger.finest(e);
                endpointManager.failedConnection(address, e, silent);
            }
        }

        private void tryConnectToIPv6() throws Exception {
            Collection<Inet6Address> possibleInetAddresses = AddressUtil
                    .getPossibleInetAddressesFor((Inet6Address) address.getInetAddress());
            Level level = silent ? Level.FINEST : Level.INFO;
            //TODO: collection.toString() will likely not produce any useful output!
            if (logger.isLoggable(level)) {
                logger.log(level, "Trying to connect possible IPv6 addresses: " + possibleInetAddresses);
            }
            boolean connected = false;
            Exception error = null;
            int configuredTimeoutMillis =
                    ioService.getSocketConnectTimeoutSeconds(endpointManager.getEndpointQualifier()) * MILLIS_PER_SECOND;
            int timeoutMillis = configuredTimeoutMillis > 0 && configuredTimeoutMillis < Integer.MAX_VALUE
                    ? configuredTimeoutMillis : DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS * MILLIS_PER_SECOND;
            for (Inet6Address inetAddress : possibleInetAddresses) {
                try {
                    tryToConnect(new InetSocketAddress(inetAddress, address.getPort()), timeoutMillis);
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

        private void tryToConnect(InetSocketAddress socketAddress, int timeout) throws Exception {
            SocketChannel socketChannel = SocketChannel.open();

            TcpIpConnection connection = null;
            Channel channel = endpointManager.newChannel(socketChannel, true);
            try {
                if (ioService.isSocketBind()) {
                    bindSocket(socketChannel);
                }

                Level level = silent ? Level.FINEST : Level.INFO;
                if (logger.isLoggable(level)) {
                    logger.log(level, "Connecting to " + socketAddress + ", timeout: " + timeout
                            + ", bind-any: " + ioService.isSocketBindAny());
                }

                try {
                    channel.connect(socketAddress, timeout);

                    ioService.interceptSocket(endpointManager.getEndpointQualifier(), socketChannel.socket(), false);

                    connection = endpointManager.newConnection(channel, address);
                    BindRequest request = new BindRequest(logger, ioService, connection, address, true);
                    request.send();
                } catch (Exception e) {
                    closeConnection(connection, e);
                    closeSocket(socketChannel);
                    logger.log(level, "Could not connect to: " + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                            + "[" + e.getMessage() + "]");
                    throw e;
                }
            } finally {
                endpointManager.removeAcceptedChannel(channel);
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
            return ioService.isSocketBindAny() ? null : ioService.getThisAddress().getInetAddress();
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
