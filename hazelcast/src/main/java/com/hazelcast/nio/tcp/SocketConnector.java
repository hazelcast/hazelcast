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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocols;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.logging.Level;

public class SocketConnector implements Runnable {

    private static final int DEFAULT_IPV6_SOCKET_CONNECT_TIMEOUT_SECONDS = 3;
    private static final int MILLIS_PER_SECOND = 1000;

    private final TcpIpConnectionManager connectionManager;
    private final Address address;
    private final ILogger logger;
    private final boolean silent;

    public SocketConnector(TcpIpConnectionManager connectionManager, Address address, boolean silent) {
        this.connectionManager = connectionManager;
        this.address = address;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
        this.silent = silent;
    }

    @Override
    public void run() {
        if (!connectionManager.isAlive()) {
            if (logger.isFinestEnabled()) {
                logger.finest("ConnectionManager is not live, connection attempt to " + address + " is cancelled!");
            }
            return;
        }

        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Starting to connect to " + address);
            }
            Address thisAddress = connectionManager.ioService.getThisAddress();
            if (address.isIPv4()) {
                // remote is IPv4; connect...
                tryToConnect(address.getInetSocketAddress(),
                        connectionManager.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND);
            } else if (thisAddress.isIPv6() && thisAddress.getScopeId() != null) {
                // Both remote and this addresses are IPv6.
                // This is a local IPv6 address and scope id is known.
                // find correct inet6 address for remote and connect...
                Inet6Address inetAddress = AddressUtil
                        .getInetAddressFor((Inet6Address) address.getInetAddress(), thisAddress.getScopeId());
                tryToConnect(new InetSocketAddress(inetAddress, address.getPort()),
                        connectionManager.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND);
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

    private void tryConnectToIPv6()
            throws Exception {
        Collection<Inet6Address> possibleInetAddresses = AddressUtil
                .getPossibleInetAddressesFor((Inet6Address) address.getInetAddress());
        Level level = silent ? Level.FINEST : Level.INFO;
        //TODO: collection.toString() will likely not produce any useful output!
        if (logger.isLoggable(level)) {
            logger.log(level, "Trying to connect possible IPv6 addresses: " + possibleInetAddresses);
        }
        boolean connected = false;
        Exception error = null;
        int configuredTimeoutMillis = connectionManager.getSocketConnectTimeoutSeconds() * MILLIS_PER_SECOND;
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
        connectionManager.initSocket(socketChannel.socket());
        if (connectionManager.ioService.isSocketBind()) {
            bindSocket(socketChannel);
        }
        Level level = silent ? Level.FINEST : Level.INFO;
        if (logger.isLoggable(level)) {
            String message = "Connecting to " + socketAddress + ", timeout: " + timeout
                    + ", bind-any: " + connectionManager.ioService.isSocketBindAny();
            logger.log(level, message);
        }
        try {
            socketChannel.configureBlocking(true);
            connectSocketChannel(socketAddress, timeout, socketChannel);
            if (logger.isFinestEnabled()) {
                logger.finest("Successfully connected to: " + address + " using socket " + socketChannel.socket());
            }
            SocketChannelWrapper socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, true);
            connectionManager.interceptSocket(socketChannel.socket(), false);

            socketChannelWrapper.configureBlocking(false);
            TcpIpConnection connection = connectionManager.assignSocketChannel(socketChannelWrapper, address);
            connection.getWriteHandler().setProtocol(Protocols.CLUSTER);
            connectionManager.sendBindRequest(connection, address, true);
        } catch (Exception e) {
            closeSocket(socketChannel);
            logger.log(level, "Could not connect to: " + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                    + "[" + e.getMessage() + "]");
            throw e;
        }
    }

    private void connectSocketChannel(InetSocketAddress socketAddress, int timeout, SocketChannel socketChannel)
            throws IOException {
        try {
            if (timeout > 0) {
                socketChannel.socket().connect(socketAddress, timeout);
            } else {
                socketChannel.connect(socketAddress);
            }
        } catch (SocketException ex) {
            //we want to include the socketAddress in the exception.
            SocketException newEx = new SocketException(ex.getMessage() + " to address " + socketAddress);
            newEx.setStackTrace(ex.getStackTrace());
            throw newEx;
        }
    }

    private void bindSocket(SocketChannel socketChannel) throws IOException {
        InetAddress inetAddress;
        if (connectionManager.ioService.isSocketBindAny()) {
            inetAddress = null;
        } else {
            Address thisAddress = connectionManager.ioService.getThisAddress();
            inetAddress = thisAddress.getInetAddress();
        }
        Socket socket = socketChannel.socket();
        if (connectionManager.useAnyOutboundPort()) {
            SocketAddress socketAddress = new InetSocketAddress(inetAddress, 0);
            socket.bind(socketAddress);
        } else {
            IOException ex = null;
            int retryCount = connectionManager.getOutboundPortCount() * 2;
            for (int i = 0; i < retryCount; i++) {
                int port = connectionManager.acquireOutboundPort();
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

    private void closeSocket(SocketChannel socketChannel) {
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (final IOException e) {
                //TODO: Why are we not using the logger?
                Logger.getLogger(SocketConnector.class).finest("Closing socket channel failed", e);
            }
        }
    }
}
