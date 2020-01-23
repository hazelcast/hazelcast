/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;

import java.util.List;

/**
 * Default address provider of Hazelcast.
 * <p>
 * Loads addresses from the Hazelcast configuration.
 */
public class DefaultAddressProvider implements AddressProvider {

    private final ClientNetworkConfig networkConfig;

    public DefaultAddressProvider(ClientNetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
    }

    @Override
    public Addresses loadAddresses() {
        List<String> configuredAddresses = networkConfig.getAddresses();

        if (configuredAddresses.isEmpty()) {
            configuredAddresses.add("127.0.0.1");
        }

        Addresses addresses = new Addresses();
        for (String address : configuredAddresses) {
            addresses.addAll(AddressHelper.getSocketAddresses(address));
        }

        return addresses;
    }

    @Override
    public Address translate(Address address) {
        return address;
    }
}
