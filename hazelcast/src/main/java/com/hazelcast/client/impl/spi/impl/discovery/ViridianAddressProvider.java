/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.management.ClientConnectionProcessListenerRunner;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;

import java.util.Collections;
import java.util.List;

public class ViridianAddressProvider implements AddressProvider {

    private final HazelcastCloudDiscovery discovery;
    private volatile HazelcastCloudDiscovery.DiscoveryResponse response
            = new HazelcastCloudDiscovery.DiscoveryResponse(Collections.emptyMap(), Collections.emptyList());

    public ViridianAddressProvider(HazelcastCloudDiscovery discovery) {
        this.discovery = discovery;
    }

    @Override
    public Addresses loadAddresses(ClientConnectionProcessListenerRunner listenerRunner) throws Exception {
        response = discovery.discoverNodes();
        List<Address> addresses = response.getPrivateMemberAddresses();
        listenerRunner.onPossibleAddressesCollected(addresses);
        return new Addresses(addresses);
    }

    @Override
    public Address translate(Address address) throws Exception {
        if (address == null) {
            return null;
        }

        Address publicAddress = response.getPrivateToPublicAddresses().get(address);
        if (publicAddress != null) {
            return publicAddress;
        }

        response = discovery.discoverNodes();

        return response.getPrivateToPublicAddresses().get(address);
    }

    @Override
    public Address translate(Member member) throws Exception {
        return translate(member.getAddress());
    }
}
