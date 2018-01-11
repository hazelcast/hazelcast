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

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.MemberAddressProvider;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;

/**
 * Delegates picking the bind and public address for this instance
 * to an implementation of {@link MemberAddressProvider}.
 */
public class DelegatingAddressPicker implements AddressPicker {
    //todo: this should be shared with DefaultAddressPicker
    private static final int SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int SOCKET_BACKLOG_LENGTH = 100;

    private final ILogger logger;
    private final MemberAddressProvider memberAddressProvider;
    private final NetworkConfig networkConfig;

    private volatile InetSocketAddress bindAddress;
    private volatile InetSocketAddress publicAddress;

    private volatile ServerSocketChannel serverSocketChannel;

    public DelegatingAddressPicker(MemberAddressProvider memberAddressProvider, NetworkConfig networkConfig, ILogger logger) {
        this.memberAddressProvider = memberAddressProvider;
        this.networkConfig = networkConfig;
        this.logger = logger;
    }


    @Override
    public void pickAddress() throws Exception {
        try {
            bindAddress = memberAddressProvider.getBindAddress();
            logger.info("Using bind address: " + bindAddress);

            publicAddress = memberAddressProvider.getPublicAddress();
            validatePublicAddress(publicAddress);
            logger.info("Using public address: " + publicAddress);

            serverSocketChannel = createServerSocketChannelInternal();

            if (publicAddress.getPort() == 0) {
                publicAddress = new InetSocketAddress(publicAddress.getAddress(), serverSocketChannel.socket().getLocalPort());
            }
        } catch (Exception e) {
            logger.severe(e);
            throw e;
        }
    }


    private ServerSocketChannel createServerSocketChannelInternal() {
        int portCount = networkConfig.getPortCount();
        int port = bindAddress.getPort() == 0 ? networkConfig.getPort() : bindAddress.getPort();
        boolean portAutoIncrement = networkConfig.isPortAutoIncrement();

        int portTrialCount = port > 0 && portAutoIncrement ? portCount : 1;
        if (port == 0) {
            logger.info("No explicit port is given, system will pick up an ephemeral port.");
        }

        IOException error = null;
        for (int i = 0; i < portTrialCount; i++) {
            InetSocketAddress tmpBindAddress = new InetSocketAddress(bindAddress.getAddress(), port + i);
            boolean reuseAddress = networkConfig.isReuseAddress();
            logger.finest("inet reuseAddress:" + reuseAddress);

            ServerSocket serverSocket = null;
            ServerSocketChannel serverSocketChannel = null;
            try {
                serverSocketChannel = ServerSocketChannel.open();
                serverSocket = serverSocketChannel.socket();
                serverSocket.setReuseAddress(reuseAddress);
                serverSocket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);

                logger.fine("Trying to bind inet socket address: " + tmpBindAddress);
                serverSocket.bind(tmpBindAddress, SOCKET_BACKLOG_LENGTH);
                logger.fine("Bind successful to inet socket address: " + serverSocket.getLocalSocketAddress());

                serverSocketChannel.configureBlocking(false);
                //todo: ugly side-effect
                bindAddress = tmpBindAddress;
                return serverSocketChannel;
            } catch (IOException e) {
                IOUtil.close(serverSocket);
                IOUtil.closeResource(serverSocketChannel);
                error = e;
            }
        }
        String message = "Cannot bind to a given address: " + bindAddress + ". Hazelcast cannot start. ";
        if (networkConfig.isPortAutoIncrement()) {
            message += "Config-port: " + networkConfig.getPort()
                    + ", latest-port: " + (port + portTrialCount);
        } else {
            message += "Port [" + port + "] is already in use and auto-increment is disabled.";
        }
        throw new HazelcastException(message, error);
    }

    private void validatePublicAddress(InetSocketAddress inetSocketAddress) {
        InetAddress address = inetSocketAddress.getAddress();
        if (address == null) {
            throw new ConfigurationException("Cannot resolve address '" + inetSocketAddress + "'");
        }

        if (address.isAnyLocalAddress()) {
            throw new ConfigurationException("Member address provider has to return a specific public address to broadcast to"
                    + " other members.");
        }
    }

    @Override
    public Address getBindAddress() {
        return new Address(bindAddress);
    }

    @Override
    public Address getPublicAddress() {
        return new Address(publicAddress);
    }

    @Override
    public ServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
    }
}
