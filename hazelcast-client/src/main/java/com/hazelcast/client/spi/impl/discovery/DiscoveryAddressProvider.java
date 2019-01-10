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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.Addresses;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DiscoveryAddressProvider
        implements AddressProvider {

    private final DiscoveryService discoveryService;

    public DiscoveryAddressProvider(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public Addresses loadAddresses() {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");

        Addresses possibleMembers = new Addresses();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            possibleMembers.primary().add(discoveryNode.getPrivateAddress());
        }
        return possibleMembers;
    }
}
