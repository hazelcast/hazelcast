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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.MemberAddressProvider;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Delegates picking the bind and public address for this instance
 * to an implementation of {@link MemberAddressProvider}.
 */
public class DelegatingAddressPicker extends AbstractAddressPicker {

    private final MemberAddressProvider memberAddressProvider;

    private InetSocketAddress bindAddress;
    private InetSocketAddress publicAddress;

    DelegatingAddressPicker(MemberAddressProvider memberAddressProvider, NetworkConfig networkConfig, ILogger logger) {
        super(networkConfig, logger);
        this.memberAddressProvider = memberAddressProvider;
    }

    @Override
    public void pickAddress() throws Exception {
        try {
            bindAddress = memberAddressProvider.getBindAddress();
            publicAddress = memberAddressProvider.getPublicAddress();
            validatePublicAddress(publicAddress);

            int port = createServerSocketChannel(bindAddress.getAddress(), bindAddress.getPort(), false);
            if (port != bindAddress.getPort()) {
                bindAddress = new InetSocketAddress(bindAddress.getAddress(), port);
            }
            logger.info("Using bind address: " + bindAddress);

            if (publicAddress.getPort() == 0) {
                publicAddress = new InetSocketAddress(publicAddress.getAddress(), port);
            }
            logger.info("Using public address: " + publicAddress);

        } catch (Exception e) {
            logger.severe(e);
            throw e;
        }
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

}
