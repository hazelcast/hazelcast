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

public class LocalAddressRegistry {
    private final Map<Address, UUID> addressToUuid;
    private final Map<UUID, LinkedAddresses> uuidToAddresses;

    public LocalAddressRegistry() {
        this.addressToUuid = new ConcurrentHashMap<>();
        this.uuidToAddresses = new ConcurrentHashMap<>();
    }

    // TODO [ufuk]: Check whether register and removeRegistration can be called
    //  concurrently for the same uuid and/or addresses or not. If so, what
    //  kind of consistency guarantee should we provide?

    // accessed from TcpServerControl#process(TcpServerConnection, MemberHandshake)
    // while member handshake processing and MembershipManager#updateMembers call
    // when a new member added event received from master
    public void register(@Nonnull UUID uuid, @Nonnull LinkedAddresses linkedAddresses) {
        // If the old linked addresses set and the new one intersect, suppose
        // that the new ones are additional addresses and add them into old
        // address set. Otherwise, If there is no intersection between these
        // two sets, I'll consider the old addresses as stale and remove them.
        LinkedAddresses previousAddresses = uuidToAddresses.get(uuid);
        if (previousAddresses != null) {
            if (previousAddresses.intersects(linkedAddresses)) {
                previousAddresses.addLinkedAddresses(linkedAddresses);
            } else {
                // override the uuid-addresses entry with the new ones (removes previous addresses)
                uuidToAddresses.put(uuid, linkedAddresses);
                // remove previous addresses from the addressToUuid map
                previousAddresses.getAllAddresses().forEach(addressToUuid::remove);
            }
        }
        linkedAddresses.getAllAddresses().forEach(address -> addressToUuid.put(address, uuid));
    }

    // Accessed from MembershipManager#updateMembers when member left event
    // received from master. In this case, we remove the related entries after
    // we have already closed our connections to that member. For the entries
    // of the connections whose remote sides are clients, after the connections
    // are closed.
    public void removeRegistration(@Nonnull UUID removedUuid, @Nonnull Address removedAddress) {
        LinkedAddresses linkedAddresses = uuidToAddresses.computeIfPresent(removedUuid, (uuid, addresses) -> {
            if (addresses.contains(removedAddress)) {
                // remove uuid to addresses entry
                return null;
            }
            return addresses;
        });

        if (linkedAddresses == null) {
            // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
            Iterator<UUID> iterator = addressToUuid.values().iterator();
            while (iterator.hasNext()) {
                UUID uuid = iterator.next();
                if (uuid.equals(removedUuid)) {
                    iterator.remove();
                }
            }
        }
    }

    @Nullable
    public UUID uuidOf(@Nonnull Address address) {
        return addressToUuid.get(address);
    }

    @Nullable
    public LinkedAddresses linkedAddressesOf(@Nonnull UUID uuid) {
        return uuidToAddresses.get(uuid);
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
}
