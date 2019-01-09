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

import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DiscoveryAddressTranslator
        implements AddressTranslator {

    private final DiscoveryService discoveryService;
    private final boolean usePublic;

    private volatile Map<Address, Address> privateToPublic;

    public DiscoveryAddressTranslator(DiscoveryService discoveryService, boolean usePublic) {
        this.discoveryService = discoveryService;
        this.usePublic = usePublic;
    }

    @Override
    public Address translate(Address address) {
        if (address == null) {
            return null;
        }
        // if it is inside cloud, return private address otherwise we need to translate it.
        if (!usePublic) {
            return address;
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
        if (publicAddress == null && !alreadyRefreshed) {
            refresh();
            privateToPublic = this.privateToPublic;
            publicAddress = privateToPublic.get(address);
        }

        // If public address available return, otherwise choose given address
        return publicAddress != null ? publicAddress : address;
    }

    @Override
    public void refresh() {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");

        Map<Address, Address> privateToPublic = new HashMap<Address, Address>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            privateToPublic.put(discoveryNode.getPrivateAddress(), discoveryNode.getPublicAddress());
        }
        this.privateToPublic = privateToPublic;
    }
}
