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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Level;

/**
 * The TcpIpConnector is responsible to make connections by connecting to a remote serverport. Once completed,
 * it will send the protocol and a bind-message.
 */
public class TcpIpConnector {

    private static final int DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS = 3;
    private static final int MILLIS_PER_SECOND = 1000;

    private final TcpIpConnectionManager connectionManager;
    private final ILogger logger;
    private final IOService ioService;
    private final int outboundPortCount;

    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();

    public TcpIpConnector(TcpIpConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLoggingService().getLogger(getClass());
        Collection<Integer> ports = ioService.getOutboundPorts();
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

        public ConnectTask(Address address, boolean silent) {
            this.address = address;
            this.silent = silent;
        }

        @Override
        public void run() {
            if (!connectionManager.isLive()) {
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
                    tryToConnect(address.getInetSocketAddress(),
                            ioService.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND);
                } else if (thisAddress.isIPv6() && thisAddress.getScopeId() != null) {
                    // Both remote and this addresses are IPv6.
                    // This is a local IPv6 address and scope ID is known.
                    // find correct inet6 address for remote and connect...
                    Inet6Address inetAddress = AddressUtil
                            .getInetAddressFor((Inet6Address) address.getInetAddress(), thisAddress.getScopeId());
                    tryToConnect(new InetSocketAddress(inetAddress, address.getPort()),
                            ioService.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND);
                } else {
                    // remote is IPv6 and this is either IPv4 or a global IPv6.
                    // find possible remote inet6 addresses and try each one to connect...
                    tryConnectToIPv6();
                }
            } catch (Throwable e) {
                logger.finest(e);
                connectionManager.failedConnection(address, e, silent);
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
            int configuredTimeoutMillis = ioService.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND;
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
            ioService.configureSocket(socketChannel.socket());
            if (ioService.isSocketBind()) {
                bindSocket(socketChannel);
            }

            Level level = silent ? Level.FINEST : Level.INFO;
            if (logger.isLoggable(level)) {
                logger.log(level, "Connecting to " + socketAddress + ", timeout: " + timeout
                        + ", bind-any: " + ioService.isSocketBindAny());
            }

            try {
                socketChannel.configureBlocking(true);
                connectSocketChannel(socketAddress, timeout, socketChannel);
                if (logger.isFinestEnabled()) {
                    logger.finest("Successfully connected to: " + address + " using socket " + socketChannel.socket());
                }
                Channel channel = connectionManager.createChannel(socketChannel, true);
                ioService.interceptSocket(socketChannel.socket(), false);
                socketChannel.configureBlocking(false);
                TcpIpConnection connection = connectionManager.newConnection(channel, address);
                connectionManager.sendBindRequest(connection, address, true);
            } catch (NullPointerException e) {
                // Helper piece of code, which will allow to identify rare NPEs in TLS connections
                // https://github.com/hazelcast/hazelcast-enterprise/issues/2104
                //TODO remove this catch block once the TLS NPE problem is successfully resolved
                closeSocket(socketChannel);
                logger.log(level, "Could not connect to: " + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                        + "[" + e.getMessage() + "]");
                logger.log(Level.INFO,
                        "Add this stacktrace to https://github.com/hazelcast/hazelcast-enterprise/issues/2104 please!", e);
                throw e;
            } catch (Exception e) {
                closeSocket(socketChannel);
                logger.log(level, "Could not connect to: " + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                        + "[" + e.getMessage() + "]");
                throw e;
            }
        }

        private void connectSocketChannel(InetSocketAddress address, int timeout, SocketChannel socketChannel)
                throws IOException {
            try {
                if (timeout > 0) {
                    socketChannel.socket().connect(address, timeout);
                } else {
                    socketChannel.connect(address);
                }
            } catch (SocketException ex) {
                //we want to include the address in the exception.
                SocketException newEx = new SocketException(ex.getMessage() + " to address " + address);
                newEx.setStackTrace(ex.getStackTrace());
                throw newEx;
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
