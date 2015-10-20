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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Default address provider of hazelcast
 * Loads addresses from hazelcast configuration
 */
public class DefaultAddressProvider implements AddressProvider {

    private ClientNetworkConfig networkConfig;

    public DefaultAddressProvider(ClientNetworkConfig networkConfig) {
        this.networkConfig = networkConfig;

    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        final List<String> addresses = networkConfig.getAddresses();
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();

        for (String address : addresses) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
        return socketAddresses;
    }
}
