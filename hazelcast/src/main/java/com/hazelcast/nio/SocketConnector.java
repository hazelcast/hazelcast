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
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.logging.Level;

public class SocketConnector implements Runnable {

    private final ConnectionManager connectionManager;
    private final Address address;
    private final ILogger logger;
    private final boolean silent;

    public SocketConnector(ConnectionManager connectionManager, Address address, boolean silent) {
        this.connectionManager = connectionManager;
        this.address = address;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
        this.silent = silent;
    }

    public void run() {
        if (!connectionManager.isLive()) {
            String message = "ConnectionManager is not live, connection attempt to " +
                             address + " is cancelled!";
            log(Level.FINEST, message);
            return;
        }
        try {
            connectionManager.ioService.onIOThreadStart();
            log(Level.FINEST, "Starting to connect to " + address);
            final Address thisAddress = connectionManager.ioService.getThisAddress();
            if (address.isIPv4()) {
                // remote is IPv4; connect...
                tryToConnect(address.getInetSocketAddress(), 0);
            } else if (thisAddress.isIPv6() && thisAddress.getScopeId() != null) {
                // Both remote and this addresses are IPv6.
                // This is a local IPv6 address and scope id is known.
                // find correct inet6 address for remote and connect...
                final Inet6Address inetAddress = AddressUtil
                        .getInetAddressFor((Inet6Address) address.getInetAddress(), thisAddress.getScopeId());
                tryToConnect(new InetSocketAddress(inetAddress, address.getPort()), 0);
            } else {
                // remote is IPv6 and this is either IPv4 or a global IPv6.
                // find possible remote inet6 addresses and try each one to connect...
                final Collection<Inet6Address> possibleInetAddresses = AddressUtil.getPossibleInetAddressesFor(
                        (Inet6Address) address.getInetAddress());
                log(Level.INFO, "Trying to connect possible IPv6 addresses: " + possibleInetAddresses);
                boolean connected = false;
                Exception error = null;
                for (Inet6Address inetAddress : possibleInetAddresses) {
                    try {
                        tryToConnect(new InetSocketAddress(inetAddress, address.getPort()), 3000);
                        connected = true;
                        break;
                    } catch (Exception e) {
                        error = e;
                    }
                }
                if (!connected) {
                    // could not connect any of addresses
                    throw error;
                }
            }
        } catch (Throwable e) {
            logger.log(Level.FINEST, e.getMessage(), e);
            connectionManager.failedConnection(address, e, silent);
        }
    }

    private void tryToConnect(final InetSocketAddress socketAddress, final int timeout)
            throws Exception {
        final SocketChannel socketChannel = SocketChannel.open();
        connectionManager.initSocket(socketChannel.socket());
        if (!connectionManager.ioService.isSocketBindAny()) {
            final Address thisAddress = connectionManager.ioService.getThisAddress();
            socketChannel.socket().bind(new InetSocketAddress(thisAddress.getInetAddress(), 0));
        }
        final String message = "Connecting to " + socketAddress
                               + ", timeout: " + timeout
                               + ", bind-any: " + connectionManager.ioService.isSocketBindAny();
        log(Level.FINEST, message);
        try {
            socketChannel.configureBlocking(true);
            if (timeout > 0) {
                socketChannel.socket().connect(socketAddress, timeout);
            } else {
                socketChannel.connect(socketAddress);
            }
            log(Level.FINEST, "Successfully connected to: " + address + " using socket " + socketChannel.socket());
            MemberSocketInterceptor memberSocketInterceptor = connectionManager.getMemberSocketInterceptor();
            if (memberSocketInterceptor != null) {
                log(Level.FINEST, "Calling member socket interceptor: " + memberSocketInterceptor
                   + " for " + socketChannel);
                memberSocketInterceptor.onConnect(socketChannel.socket());
            }
            socketChannel.configureBlocking(false);
            final SocketChannelWrapper socketChannelWrapper = connectionManager
                    .wrapSocketChannel(socketChannel, true);
            Connection connection = connectionManager.assignSocketChannel(socketChannelWrapper);
            connectionManager.sendBindRequest(connection, address, true);
        } catch (Exception e) {
            closeSocket(socketChannel);
            final Level level = silent ? Level.FINEST : Level.INFO;
            log(level, "Could not connect to: "
                                      + socketAddress + ". Reason: " + e.getClass().getSimpleName()
                                      + "[" + e.getMessage() + "]");
            throw e;
        }
    }

    private void closeSocket(final SocketChannel socketChannel) {
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (final IOException ignored) {
            }
        }
    }

    private void log(Level level, String message) {
        logger.log(level, message);
        connectionManager.ioService.getSystemLogService().logConnection(message);
    }
}
