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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;

import com.hazelcast.logging.Logger;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class PrivateLinkAddressTranslator implements AddressProvider {

    private final ClientNetworkConfig networkConfig;
    private final String[] privateLinkOrderedZonalNames;

    public PrivateLinkAddressTranslator(ClientNetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
        List<String> zonalNames = networkConfig.getPrivateLinkOrderedZonalNames();
        this.privateLinkOrderedZonalNames = zonalNames.toArray(new String[zonalNames.size()]);
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

        String[] ip = address.getHost().split("\\.");           
        String zonalName = privateLinkOrderedZonalNames[Integer.parseInt(ip[2])];
        int port = Integer.parseInt(ip[2]) * 256 + Integer.parseInt(ip[3]);

        Address pvtlnAddr;
        try {
            pvtlnAddr = new Address(InetAddress.getByName(zonalName), port);
        } catch (UnknownHostException ignored) {
            Logger.getLogger(PrivateLinkAddressTranslator.class).severe("Address not available", ignored);
            return null;
        }        

        return pvtlnAddr;
    }

}