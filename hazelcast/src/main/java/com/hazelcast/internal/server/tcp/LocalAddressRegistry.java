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

    public void register(UUID uuid, Address address) {
        addressToUuid.put(address, uuid);
        if (uuidToAddresses.containsKey(uuid)) {
            uuidToAddresses.get(uuid).addLinkedAddress(address);
        } else {
            uuidToAddresses.put(uuid, LinkedAddresses.getAllLinkedAddresses(address));
        }
    }

    public void removeRegistration(UUID removedUuid) {
        // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
        Iterator<UUID> iterator = addressToUuid.values().iterator();
        while (iterator.hasNext()) {
            UUID uuid = iterator.next();
            if (uuid.equals(removedUuid)) {
                iterator.remove();
            }
        }
        uuidToAddresses.remove(removedUuid);
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
