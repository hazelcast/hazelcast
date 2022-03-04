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

import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.MemberAddressProvider;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.ServerSocketHelper.createServerSocketChannel;

/**
 * Delegates picking the bind and public address for this instance
 * to an implementation of {@link MemberAddressProvider}.
 */
final class DelegatingAddressPicker
        implements AddressPicker {

    private final Map<EndpointQualifier, InetSocketAddress> bindAddresses = new ConcurrentHashMap<>();

    private final Map<EndpointQualifier, InetSocketAddress> publicAddresses = new ConcurrentHashMap<>();

    private final Map<EndpointQualifier, ServerSocketChannel> serverSocketChannels = new ConcurrentHashMap<>();

    private final MemberAddressProvider memberAddressProvider;
    private final Config config;
    private final ILogger logger;
    private final boolean usesAdvancedNetworkConfig;

    DelegatingAddressPicker(MemberAddressProvider memberAddressProvider, Config config, ILogger logger) {
        super();
        this.logger = logger;
        this.config = config;
        this.memberAddressProvider = memberAddressProvider;
        this.usesAdvancedNetworkConfig = config.getAdvancedNetworkConfig().isEnabled();
    }

    @Override
    public void pickAddress() throws Exception {
        try {
            if (usesAdvancedNetworkConfig) {
                pickAddressFromEndpointConfig();
            } else {
                // just use pre-3.12 network config
                pickAddressFromNetworkConfig();
            }

        } catch (Exception e) {
            logger.severe(e);
            throw e;
        }
    }

    private void validatePublicAddress(InetSocketAddress inetSocketAddress) {
        InetAddress address = inetSocketAddress.getAddress();
        if (address == null) {
            throw new InvalidConfigurationException("Cannot resolve address '" + inetSocketAddress + "'");
        }

        if (address.isAnyLocalAddress()) {
            throw new InvalidConfigurationException("Member address provider has to return a specific public address "
                    + "to broadcast to other members.");
        }
    }

    private void pickAddressFromNetworkConfig() {
        InetSocketAddress bindAddress;
        InetSocketAddress publicAddress;
        ServerSocketChannel serverSocketChannel;

        NetworkConfig networkConfig = config.getNetworkConfig();
        bindAddress = memberAddressProvider.getBindAddress();
        publicAddress = memberAddressProvider.getPublicAddress();
        validatePublicAddress(publicAddress);

        serverSocketChannel = createServerSocketChannel(logger, null, bindAddress.getAddress(),
                bindAddress.getPort() == 0 ? networkConfig.getPort() : bindAddress.getPort(), networkConfig.getPortCount(),
                networkConfig.isPortAutoIncrement(), networkConfig.isReuseAddress(), false);

        int port = serverSocketChannel.socket().getLocalPort();
        if (port != bindAddress.getPort()) {
            bindAddress = new InetSocketAddress(bindAddress.getAddress(), port);
        }
        logger.info("Using bind address: " + bindAddress);

        if (publicAddress.getPort() == 0) {
            publicAddress = new InetSocketAddress(publicAddress.getAddress(), port);
        }
        logger.info("Using public address: " + publicAddress);

        bindAddresses.put(MEMBER, bindAddress);
        publicAddresses.put(MEMBER, publicAddress);
        serverSocketChannels.put(MEMBER, serverSocketChannel);
    }

    private void pickAddressFromEndpointConfig() {
        InetSocketAddress bindAddress;
        InetSocketAddress publicAddress;
        ServerSocketChannel serverSocketChannel;

        for (EndpointConfig config : config.getAdvancedNetworkConfig().getEndpointConfigs().values()) {
            if (!(config instanceof ServerSocketEndpointConfig)) {
                continue;
            }
            ServerSocketEndpointConfig endpointConfig = (ServerSocketEndpointConfig) config;
            EndpointQualifier qualifier = endpointConfig.getQualifier();

            bindAddress = memberAddressProvider.getBindAddress(qualifier);
            publicAddress = memberAddressProvider.getPublicAddress(qualifier);
            validatePublicAddress(publicAddress);

            if (!bindAddresses.values().contains(bindAddress)) {
                // bind new server socket
                serverSocketChannel = createServerSocketChannel(logger, config, bindAddress.getAddress(),
                        bindAddress.getPort() == 0 ? endpointConfig.getPort() : bindAddress.getPort(),
                        endpointConfig.getPortCount(), endpointConfig.isPortAutoIncrement(),
                        endpointConfig.isReuseAddress(), false);

                serverSocketChannels.put(qualifier, serverSocketChannel);

                int port = serverSocketChannel.socket().getLocalPort();
                if (port != bindAddress.getPort()) {
                    bindAddress = new InetSocketAddress(bindAddress.getAddress(), port);
                }
                if (publicAddress.getPort() == 0) {
                    publicAddress = new InetSocketAddress(publicAddress.getAddress(), port);
                }
            }

            logger.info("Using bind address: " + bindAddress + ", "
                      + "public address: " + publicAddress + " for qualifier " + qualifier);

            bindAddresses.put(qualifier, bindAddress);
            publicAddresses.put(qualifier, publicAddress);
        }
    }

    @Override
    public Address getBindAddress(EndpointQualifier qualifier) {
        return usesAdvancedNetworkConfig
                ? new Address(bindAddresses.get(qualifier))
                : new Address(bindAddresses.get(MEMBER));
    }

    @Override
    public Address getPublicAddress(EndpointQualifier qualifier) {
        return usesAdvancedNetworkConfig
                ? new Address(publicAddresses.get(qualifier))
                : new Address(publicAddresses.get(MEMBER));
    }

    @Override
    public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
        return usesAdvancedNetworkConfig
                ? serverSocketChannels.get(qualifier)
                : serverSocketChannels.get(MEMBER);
    }

    @Override
    public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
        return serverSocketChannels;
    }

    @Override
    public Map<EndpointQualifier, Address> getPublicAddressMap() {
        Map<EndpointQualifier, Address> mappings = new HashMap<>(publicAddresses.size());
        for (Map.Entry<EndpointQualifier, InetSocketAddress> entry : publicAddresses.entrySet()) {
            mappings.put(entry.getKey(), new Address(entry.getValue()));
        }
        return mappings;
    }

    @Override
    public Map<EndpointQualifier, Address> getBindAddressMap() {
        Map<EndpointQualifier, Address> mappings = new HashMap<>(bindAddresses.size());
        for (Map.Entry<EndpointQualifier, InetSocketAddress> entry : bindAddresses.entrySet()) {
            mappings.put(entry.getKey(), new Address(entry.getValue()));
        }
        return mappings;
    }
}
