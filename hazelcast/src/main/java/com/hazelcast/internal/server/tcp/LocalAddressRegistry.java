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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalAddressRegistry {
    private final Map<Address, UUID> addressToUuid;
    private final Map<UUID, Pair> uuidToAddresses;

    public LocalAddressRegistry() {
        this.addressToUuid = new ConcurrentHashMap<>();
        this.uuidToAddresses = new ConcurrentHashMap<>();
    }

    // accessed from TcpServerControl#process(TcpServerConnection, MemberHandshake)
    // while member handshake processing and MembershipManager#updateMembers call
    // when a new member added event received from master
    public void register(@Nonnull UUID memberUuid, @Nonnull LinkedAddresses linkedAddresses) {
        // If the old linked addresses set and the new one intersect, suppose
        // that the new ones are additional addresses and add them into old
        // address set. Otherwise, If there is no intersection between these
        // two sets, I'll consider the old addresses as stale and remove them.
        uuidToAddresses.compute(memberUuid, (uuid, linkedAddressesConnectionCountPair) -> {
            if (linkedAddressesConnectionCountPair == null) {
                linkedAddressesConnectionCountPair = new Pair(linkedAddresses, new AtomicInteger(1));
            } else {
                LinkedAddresses previousAddresses = linkedAddressesConnectionCountPair.getAddresses();
                AtomicInteger connectionCount = linkedAddressesConnectionCountPair.connectionCount;

                if (previousAddresses.intersects(linkedAddresses)) {
                    previousAddresses.addLinkedAddresses(linkedAddresses);
                    connectionCount.incrementAndGet();
                } else {
                    // override the value pair with the new one (removes previous addresses)
                    linkedAddressesConnectionCountPair = new Pair(linkedAddresses, new AtomicInteger(1));
                    // remove previous addresses from the addressToUuid map
                    previousAddresses.getAllAddresses().forEach(addressToUuid::remove);
                }
            }
            linkedAddresses.getAllAddresses().forEach(address -> addressToUuid.put(address, memberUuid));

            return linkedAddressesConnectionCountPair;
        });
    }

    // Accessed from MembershipManager#updateMembers when member left event
    // received from master. In this case, we remove the related entries after
    // we have already closed our connections to that member. For the entries
    // of the connections whose remote sides are clients, after the connections
    // are closed.
    public void tryRemoveRegistration(@Nonnull UUID memberUuid, @Nonnull Address primaryAddress) {
        uuidToAddresses.computeIfPresent(memberUuid, (uuid, linkedAddressesConnectionCountPair) -> {
            LinkedAddresses addresses = linkedAddressesConnectionCountPair.getAddresses();
            if (addresses.contains(primaryAddress)) {
                AtomicInteger connectionCount = linkedAddressesConnectionCountPair.getConnectionCount();
                // there is no active connection after this remove the entry
                if (connectionCount.decrementAndGet() == 0) {
                    // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
                    Iterator<UUID> iterator = addressToUuid.values().iterator();
                    while (iterator.hasNext()) {
                        UUID currUuid = iterator.next();
                        if (currUuid.equals(memberUuid)) {
                            iterator.remove();
                        }
                    }
                    // remove the entry
                    return null;
                }
            }
            return linkedAddressesConnectionCountPair;
        });
    }

    @Nullable
    public UUID uuidOf(@Nonnull Address address) {
        return addressToUuid.get(address);
    }

    @Nullable
    public LinkedAddresses linkedAddressesOf(@Nonnull UUID uuid) {
        Pair pair = uuidToAddresses.get(uuid);
        return pair != null ? pair.getAddresses() : null;
    }

    @Nullable
    public Address getPrimaryAddress(@Nonnull UUID uuid) {
        LinkedAddresses linkedAddresses = linkedAddressesOf(uuid);
        return linkedAddresses != null ? linkedAddresses.getPrimaryAddress() : null;
    }

    @Nullable
    public Address getPrimaryAddress(@Nonnull Address address) {
        UUID uuid = uuidOf(address);
        return uuid != null ? getPrimaryAddress(uuid) : null;
    }

    public void reset() {
        addressToUuid.clear();
        uuidToAddresses.clear();
    }

    private static final class Pair {
        private final LinkedAddresses addresses;
        private final AtomicInteger connectionCount;

        private Pair(LinkedAddresses addresses, AtomicInteger connectionCount) {
            this.addresses = addresses;
            this.connectionCount = connectionCount;
        }

        public LinkedAddresses getAddresses() {
            return addresses;
        }

        public AtomicInteger getConnectionCount() {
            return connectionCount;
        }
    }
}
