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
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

public class SocketConnector implements Runnable {

    private final ConnectionManager connectionManager;
    private final Address address;
    private final ILogger logger;

    public SocketConnector(ConnectionManager connectionManager, Address address) {
        this.connectionManager = connectionManager;
        this.address = address;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
    }

    public void run() {
        if (!connectionManager.isLive()) {
            logger.log(Level.FINEST, "ConnectionManager is not live, connection attempt to " +
                    address + " is cancelled!");
            return;
        }
        SocketChannel socketChannel = null;
        try {
            connectionManager.ioService.onIOThreadStart();
            socketChannel = SocketChannel.open();
            final Address thisAddress = connectionManager.ioService.getThisAddress();
            connectionManager.initSocket(socketChannel.socket());
            socketChannel.socket().bind(new InetSocketAddress(thisAddress.getInetAddress(), 0));
            logger.log(Level.FINEST, "connecting to " + address);
            boolean connected = socketChannel.connect(getRemoteSocketAddress(address));
            logger.log(Level.FINEST, "connection check. connected: " + connected + ", " + address);
            MemberSocketInterceptor memberSocketInterceptor = connectionManager.getMemberSocketInterceptor();
            if (memberSocketInterceptor != null) {
                memberSocketInterceptor.onConnect(socketChannel.socket());
            }
            socketChannel.configureBlocking(false);
            logger.log(Level.FINEST, "connected to " + address);
            final SocketChannelWrapper socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, true);
            Connection connection = connectionManager.assignSocketChannel(socketChannelWrapper);
            connectionManager.bind(address, connection, false);
        } catch (Throwable e) {
            logger.log(Level.FINEST, e.getMessage(), e);
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (final IOException ignored) {
                }
            }
            connectionManager.failedConnection(address, e);
        }
    }

    private InetSocketAddress getRemoteSocketAddress(Address address) throws UnknownHostException {
        InetAddress inetAddress = address.getInetAddress();
        if (inetAddress instanceof Inet6Address) {
            if (inetAddress.isLinkLocalAddress() || inetAddress.isSiteLocalAddress()) {
                inetAddress = Inet6Address.getByName(prepareHostAddress(address));
            }
        }
        return new InetSocketAddress(inetAddress, address.getPort());
    }

    private String prepareHostAddress(Address address) {
        String hostAddress = address.getHost();
        if (address.isIPv6() && connectionManager.ipV6ScopeId != null) {
            hostAddress += "%" + connectionManager.ipV6ScopeId;
        }
        return hostAddress;
    }
}
