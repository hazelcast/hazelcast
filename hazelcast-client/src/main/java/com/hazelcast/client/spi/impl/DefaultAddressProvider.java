/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Default address provider of Hazelcast.
 *
 * Loads addresses from the Hazelcast configuration.
 */
public class DefaultAddressProvider implements AddressProvider {

    private ClientNetworkConfig networkConfig;
    private boolean noOtherAddressProviderExist;

    public DefaultAddressProvider(ClientNetworkConfig networkConfig, boolean noOtherAddressProviderExist) {
        this.networkConfig = networkConfig;
        this.noOtherAddressProviderExist = noOtherAddressProviderExist;
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        final List<String> addresses = networkConfig.getAddresses();
        if (addresses.isEmpty() && noOtherAddressProviderExist) {
            addresses.add("localhost");
        }
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();

        for (String address : addresses) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
        return socketAddresses;
    }
}
