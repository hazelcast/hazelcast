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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.nio.Address;

import java.nio.channels.ServerSocketChannel;
import java.util.Map;

import static java.util.Collections.singletonMap;

class StaticAddressPicker implements AddressPicker {

    private final Address thisAddress;

    StaticAddressPicker(Address thisAddress) {
        this.thisAddress = thisAddress;
    }

    public void pickAddress() {
    }

    @Override
    public Address getBindAddress() {
        return thisAddress;
    }

    @Override
    public Address getBindAddress(EndpointQualifier qualifier) {
        return thisAddress;
    }

    @Override
    public Address getPublicAddress() {
        return thisAddress;
    }

    @Override
    public Address getPublicAddress(EndpointQualifier qualifier) {
        return thisAddress;
    }

    @Override
    public Map<EndpointQualifier, Address> getPublicAddressMap() {
        return singletonMap(EndpointQualifier.MEMBER, thisAddress);
    }

    @Override
    public ServerSocketChannel getServerSocketChannel() {
        return null;
    }

    @Override
    public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
        return null;
    }

    @Override
    public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
        return null;
    }

}
