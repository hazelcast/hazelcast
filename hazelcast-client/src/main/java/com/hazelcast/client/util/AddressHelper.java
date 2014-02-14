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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.util.AddressUtil.getPossibleInetAddressesFor;

public final class AddressHelper {

    private static final int MAX_PORT_TRIES = 3;

    public static Collection<InetSocketAddress> getSocketAddresses(String address) {
        final AddressHolder addressHolder = AddressUtil.getAddressHolder(address, -1);
        final String scopedAddress = addressHolder.scopeId != null
                ? addressHolder.address + "%" + addressHolder.scopeId
                : addressHolder.address;
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByName(scopedAddress);
        } catch (UnknownHostException ignored) {
        }
        return getPossibleSocketAddresses(inetAddress, addressHolder.port, scopedAddress);
    }

    public static Collection<InetSocketAddress> getPossibleSocketAddresses(
            InetAddress inetAddress, int port, String scopedAddress) {
        int portTryCount = 1;
        if (port == -1) {
            portTryCount = MAX_PORT_TRIES;
            port = 5701;
        }
        final Collection<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        if (inetAddress == null) {
            for (int i = 0; i < portTryCount; i++) {
                socketAddresses.add(new InetSocketAddress(scopedAddress, port + i));
            }
        } else if (inetAddress instanceof Inet4Address) {
            for (int i = 0; i < portTryCount; i++) {
                socketAddresses.add(new InetSocketAddress(inetAddress, port + i));
            }
        } else if (inetAddress instanceof Inet6Address) {
            final Collection<Inet6Address> addresses = getPossibleInetAddressesFor((Inet6Address) inetAddress);
            for (Inet6Address inet6Address : addresses) {
                for (int i = 0; i < portTryCount; i++) {
                    socketAddresses.add(new InetSocketAddress(inet6Address, port + i));
                }
            }
        }

        return socketAddresses;
    }

    private AddressHelper() {
    }
}
