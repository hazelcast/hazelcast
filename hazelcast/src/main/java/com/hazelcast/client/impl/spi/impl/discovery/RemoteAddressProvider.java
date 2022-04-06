/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class RemoteAddressProvider implements AddressProvider {

    private final Callable<Map<Address, Address>> getAddresses;
    private final boolean usePublic;
    private volatile Map<Address, Address> privateToPublic = new HashMap<Address, Address>();

    public RemoteAddressProvider(Callable<Map<Address, Address>> getAddresses, boolean usePublic) {
        this.getAddresses = getAddresses;
        this.usePublic = usePublic;
    }

    @Override
    public Addresses loadAddresses() throws Exception {
        privateToPublic = getAddresses.call();
        return new Addresses(privateToPublic.keySet());
    }

    @Override
    public Address translate(Address address) throws Exception {
        if (address == null) {
            return null;
        }

        // if it is inside cloud, return private address otherwise we need to translate it.
        if (!usePublic) {
            return address;
        }

        Address publicAddress = privateToPublic.get(address);
        if (publicAddress != null) {
            return publicAddress;
        }

        privateToPublic = getAddresses.call();

        return privateToPublic.get(address);
    }

    @Override
    public Address translate(Member member) throws Exception {
        return translate(member.getAddress());
    }
}
