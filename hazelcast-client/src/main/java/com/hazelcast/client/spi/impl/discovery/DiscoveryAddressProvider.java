/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

public class DiscoveryAddressProvider
        implements AddressProvider {

    private static final ILogger LOGGER = Logger.getLogger(DiscoveryAddressProvider.class);

    private final DiscoveryService discoveryService;

    public DiscoveryAddressProvider(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        Iterable<DiscoveryNode> discoveredNodes = discoveryService.discoverNodes();

        Collection<InetSocketAddress> possibleMembers = new ArrayList<InetSocketAddress>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            Address discoveredAddress = discoveryNode.getPrivateAddress();
            try {
                possibleMembers.add(discoveredAddress.getInetSocketAddress());
            } catch (UnknownHostException e) {
                LOGGER.warning("Unresolvable host exception", e);
            }
        }
        return possibleMembers;
    }
}
