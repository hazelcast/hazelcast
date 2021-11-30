/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * LinkedAddresses keeps all network addresses pointing to the same Hazelcast
 * instance together. Also, it specifically stores which of these addresses is
 * the primary address.
 */
public final class LinkedAddresses {
    private final Address primaryAddress;
    // TODO [ufuk]: consider other kinds of data structures for storing linkedAddresses (esp. for duplicate elimination)
    private final List<Address> linkedAddresses;

    LinkedAddresses(Address primaryAddress) {
        this(primaryAddress, new LinkedList<>());
    }

    private LinkedAddresses(Address primaryAddress, List<Address> linkedAddresses) {
        this.primaryAddress = requireNonNull(primaryAddress);
        this.linkedAddresses = requireNonNull(linkedAddresses);
    }

    public Address getPrimaryAddress() {
        return primaryAddress;
    }

    public void addLinkedAddress(Address address) {
        linkedAddresses.add(address);
    }

    public static LinkedAddresses getLinkedAddresses(Address address) {
        try {
            InetAddress inetAddress = address.getInetAddress();

            String host = address.getHost();
            String canonicalHost = inetAddress.getCanonicalHostName();
            String ip = inetAddress.getHostAddress();

            Address addressHost = new Address(host, address.getPort());
            Address addressIp = new Address(ip, address.getPort());
            Address addressCanonicalHost = new Address(canonicalHost, address.getPort());
            return new LinkedAddresses(addressHost,
                    asList(addressIp, addressCanonicalHost));
        } catch (UnknownHostException e) {
            // we have a hostname here in `address`, but we can't resolve it
            // how on earth we could come here?
            ignore(e);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LinkedAddresses that = (LinkedAddresses) o;

        return Objects.equals(primaryAddress, that.primaryAddress);
    }

    @Override
    public int hashCode() {
        return primaryAddress != null ? primaryAddress.hashCode() : 0;
    }

    public boolean contains(Address address) {
        return primaryAddress.equals(address) || linkedAddresses.contains(address);
    }

    @Override
    public String toString() {
        return "LinkedAddresses{"
                + "primaryAddress=" + primaryAddress
                + ", linkedAddresses=" + linkedAddresses
                + '}';
    }
}
