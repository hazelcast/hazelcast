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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;

/**
 * LinkedAddresses keeps all network addresses pointing to the same Hazelcast
 * instance together. Also, it specifically stores which of these addresses is
 * the primary address.
 */
public final class LinkedAddresses {

    private final Address primaryAddress;

    // all linked addresses also includes primary address
    private final Set<Address> allLinkedAddresses;

    LinkedAddresses(Address primaryAddress) {
        this.primaryAddress = primaryAddress;
        allLinkedAddresses = newSetFromMap(new ConcurrentHashMap<>());
        allLinkedAddresses.add(primaryAddress);
    }

    public Address getPrimaryAddress() {
        return primaryAddress;
    }

    public Set<Address> getAllAddresses() {
        return unmodifiableSet(allLinkedAddresses);
    }

    public void addAllResolvedAddresses(Address address) {
        this.addLinkedAddresses(getResolvedAddresses(address));
    }

    public void addLinkedAddresses(LinkedAddresses other) {
        allLinkedAddresses.addAll(other.getAllAddresses());
    }

    public static LinkedAddresses getResolvedAddresses(Address primaryAddress) {
        LinkedAddresses linkedAddresses = new LinkedAddresses(primaryAddress);
        try {
            InetAddress inetAddress = primaryAddress.getInetAddress();
            // ip address for the given primary address
            String ip = inetAddress.getHostAddress();

            Address addressIp = new Address(ip, primaryAddress.getPort());
            linkedAddresses.addAddress(addressIp);
        } catch (UnknownHostException e) {
            // we have a hostname here in `address`, but we can't resolve it
            // how on earth we could come here?
            ignore(e);
        }
        return linkedAddresses;
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

        return Objects.equals(primaryAddress, that.primaryAddress)
                && allLinkedAddresses.size() == that.allLinkedAddresses.size()
                && allLinkedAddresses.containsAll(that.allLinkedAddresses);
    }

    @Override
    public int hashCode() {
        return primaryAddress != null ? primaryAddress.hashCode() : 0;
    }

    public boolean contains(Address address) {
        return allLinkedAddresses.contains(address);
    }

    public boolean intersects(LinkedAddresses other) {
        Set<Address> tmp = new HashSet<>(allLinkedAddresses);
        tmp.retainAll(other.getAllAddresses());
        return tmp.size() > 0;
    }

    @Override
    public String toString() {
        return "LinkedAddresses{"
                + "primaryAddress=" + primaryAddress
                + ", allLinkedAddresses=" + allLinkedAddresses
                + '}';
    }

    private void addAddress(Address address) {
        allLinkedAddresses.add(address);
    }
}
