/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.AddressUtil.AddressHolder;

import java.net.*;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * @mdogan 4/6/12
 */
public final class AddressHelper {

    public static Collection<InetSocketAddress> getSocketAddresses(String address) {
        final AddressHolder addressHolder = AddressUtil.getAddressHolder(address, 5701);
        final String scopedAddress = addressHolder.scopeId != null
                                     ? addressHolder.address + "%" + addressHolder.scopeId
                                     : addressHolder.address;
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByName(scopedAddress);
        } catch (UnknownHostException ignored) {
        }
        if (inetAddress == null) {
            return Collections.singleton(new InetSocketAddress(scopedAddress, addressHolder.port));
        }
        return getPossibleSocketAddresses(inetAddress, addressHolder.port);
    }

    public static Collection<InetSocketAddress> getPossibleSocketAddresses(InetAddress inetAddress, int port) {
        if (inetAddress == null) {
            return Collections.emptySet();
        }
        if (inetAddress instanceof Inet4Address) {
            return Collections.singleton(new InetSocketAddress(inetAddress, port));
        }
        final Collection<Inet6Address> addresses = AddressUtil.getPossibleInetAddressesFor((Inet6Address) inetAddress);
        final Collection<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        for (Inet6Address inet6Address : addresses) {
            socketAddresses.add(new InetSocketAddress(inet6Address, port));
        }
        return socketAddresses;
    }

    private AddressHelper() {}
}
