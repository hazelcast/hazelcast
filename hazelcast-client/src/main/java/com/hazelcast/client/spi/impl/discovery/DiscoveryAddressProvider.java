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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DiscoveryAddressProvider
        implements AddressProvider {

    private final ILogger logger;
    private final DiscoveryService discoveryService;

    public DiscoveryAddressProvider(DiscoveryService discoveryService, LoggingService loggingService) {
        this.discoveryService = discoveryService;
        logger = loggingService.getLogger(DiscoveryAddressProvider.class);
    }

    @Override
    public Collection<Address> loadAddresses() {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");

        Collection<Address> possibleMembers = new ArrayList<Address>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            possibleMembers.add(discoveryNode.getPrivateAddress());
        }
        return possibleMembers;
    }
}
