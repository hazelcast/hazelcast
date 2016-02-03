/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.HashMap;
import java.util.Map;

public class DiscoveryAddressTranslator
        implements AddressTranslator {

    private final DiscoveryService discoveryService;

    private volatile Map<Address, Address> privateToPublic;

    public DiscoveryAddressTranslator(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    public Address translate(Address address) {
        if (address == null) {
            return null;
        }

        // Refresh only once to prevent load on service discovery
        boolean alreadyRefreshed = false;

        Map<Address, Address> privateToPublic = this.privateToPublic;
        if (privateToPublic == null) {
            refresh();
            alreadyRefreshed = true;
        }

        privateToPublic = this.privateToPublic;
        Address publicAddress = privateToPublic.get(address);
        if (!alreadyRefreshed) {
            refresh();
            privateToPublic = this.privateToPublic;
            publicAddress = privateToPublic.get(address);
        }

        // If public address available return, otherwise choose given address
        return publicAddress != null ? publicAddress : address;
    }

    @Override
    public void refresh() {
        Iterable<DiscoveryNode> discoveredNodes = discoveryService.discoverNodes();

        Map<Address, Address> privateToPublic = new HashMap<Address, Address>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            privateToPublic.put(discoveryNode.getPrivateAddress(), discoveryNode.getPublicAddress());
        }
        this.privateToPublic = privateToPublic;
    }
}
