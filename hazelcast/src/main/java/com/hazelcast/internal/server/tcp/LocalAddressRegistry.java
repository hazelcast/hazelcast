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
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * A LocalAddressRegistry contains maps to store `UUID -> Addresses`
 * and its reverse `Address->UUID` mappings which is used to manage
 * addresses of a Hazelcast instance.
 */
public class LocalAddressRegistry {
    private final Map<Address, UUID> addressToUuid;
    private final Map<UUID, Pair> uuidToAddresses;
    private final ILogger logger;

    // Since the requested lifecycle of local member's uuid and addresses are slightly different
    // from the remote ones, I manage these separately.
    private volatile UUID localUuid;
    private volatile LinkedAddresses localAddresses;

    // protected for testing purposes
    protected LocalAddressRegistry(ILogger logger) {
        this.addressToUuid = new ConcurrentHashMap<>();
        this.uuidToAddresses = new ConcurrentHashMap<>();
        this.logger = logger;
    }

    public LocalAddressRegistry(Node node, AddressPicker addressPicker) {
        this(node.getLogger(LocalAddressRegistry.class));
        registerLocalAddresses(node.getThisUuid(), addressPicker);
    }

    /**
     * Binds a set of address to given instance uuid. While registering these
     * addresses, we use the LinkedAddresses, it stores one of these addresses
     * as the primary address.
     * We count the registrations made to the same instance uuid, and we require
     * to removal as much as this registration count in order to delete a
     * registry entry. If multiple registration is performed on the same uuid
     * (it can happen when there are multiple connections between the members),
     * we require all the connections are closed to remove the registration entry.
     * <p>
     * When multiple registration attempts are made to the same uuid, it goes like this:
     * If the registration to the same uuid includes an address set that intersects
     * with the current registered addresses, we combine these two address set and
     * update the entry with this combined value. Also, then increment the registration
     * count of the registry entry.
     *
     * If the registration to the same uuid includes a completely different
     * address set than the already registered addresses, we call the old
     * registration stale and remove it completely. In this case, we reset the
     * registration count to 1.
     *
     * @param instanceUuid the uuid of instance (member or client)
     * @param linkedAddresses a set of addresses
     */
    public void register(@Nonnull UUID instanceUuid, @Nonnull LinkedAddresses linkedAddresses) {
        // If the old linked addresses set and the new one intersect, suppose
        // that the new ones are additional addresses and add them into old
        // address set. Otherwise, If there is no intersection between these
        // two sets, I'll consider the old addresses as stale and remove them.
        uuidToAddresses.compute(instanceUuid, (uuid, linkedAddressesRegistrationCountPair) -> {
            if (linkedAddressesRegistrationCountPair == null) {
                linkedAddressesRegistrationCountPair = new Pair(linkedAddresses, new AtomicInteger(1));
            } else {
                LinkedAddresses previousAddresses = linkedAddressesRegistrationCountPair.getAddresses();
                AtomicInteger registrationCount = linkedAddressesRegistrationCountPair.registrationCount;

                if (previousAddresses.intersects(linkedAddresses)) {
                    previousAddresses.addLinkedAddresses(linkedAddresses);
                    registrationCount.incrementAndGet();
                } else {
                    // override the value pair with the new one (removes previous addresses)
                    linkedAddressesRegistrationCountPair = new Pair(linkedAddresses, new AtomicInteger(1));
                    // remove previous addresses from the addressToUuid map
                    previousAddresses.getAllAddresses().forEach(address -> addressToUuid.remove(address, uuid));
                }
            }
            linkedAddresses.getAllAddresses().forEach(address -> addressToUuid.put(address, instanceUuid));
            if (logger.isFinestEnabled()) {
                logger.finest(linkedAddresses + " registered for the instance uuid=" + instanceUuid
                        + " currently all registered addresses for this instance uuid: "
                        + linkedAddressesRegistrationCountPair.getAddresses());
            }
            return linkedAddressesRegistrationCountPair;
        });
    }

    /**
     * Try to remove the registry entry for given instance uuid and primary address.
     * To make sure not delete a new entry of a rejoined member with the same uuid
     * by a stale connection close event, both member uuid and primary address of
     * the connection that was closed is checked if it matches with the entry inside
     * the address registry.
     * <p>
     * If the uuid and addresses match some entry in the address registry, we try to
     * remove this entry if the all connections belong to this member is closed. We
     * keep track the number of active connections belongs to this member uuid entry
     * and remove if there is no active connection to this member uuid.
     *
     * @param instanceUuid    instance uuid of which we try to remove its registration entry
     * @param primaryAddress  primary address which is set as a Connection#remoteAddress
     *                       to remove the registration entry
     */
    public void tryRemoveRegistration(@Nonnull UUID instanceUuid, @Nonnull Address primaryAddress) {
        uuidToAddresses.computeIfPresent(instanceUuid, (uuid, linkedAddressesRegistrationCountPair) -> {
            LinkedAddresses addresses = linkedAddressesRegistrationCountPair.getAddresses();
            if (addresses.contains(primaryAddress)) {
                AtomicInteger registrationCount = linkedAddressesRegistrationCountPair.getRegistrationCount();
                // there is no active connection after this remove the entry
                if (registrationCount.decrementAndGet() == 0) {
                    // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
                    Iterator<UUID> iterator = addressToUuid.values().iterator();
                    while (iterator.hasNext()) {
                        UUID currUuid = iterator.next();
                        if (currUuid.equals(instanceUuid)) {
                            iterator.remove();
                        }
                    }
                    if (logger.isFineEnabled()) {
                        logger.fine(addresses + " previously registered for the instance uuid=" + instanceUuid
                                + " are removed from the registry");
                    }
                    // remove the entry
                    return null;
                }
            }
            return linkedAddressesRegistrationCountPair;
        });
    }

    /**
     * If this address or its resolved IP address has been registered before,
     * it returns the instance uuid corresponding to this address.
     * @param address address of hz instance
     * @return the registered instance uuid corresponds to given address,
     *          null if the address is not registered
     */
    @Nullable
    public UUID uuidOf(@Nonnull Address address) {
        try {
            // try to resolve this address first
            address = new Address(address.getInetSocketAddress());
        } catch (UnknownHostException e) {
            ignore(e);
        }
        if (localAddresses != null && localAddresses.contains(address)) {
            return localUuid;
        }
        return addressToUuid.get(address);
    }

    /**
     * If this instance uuid and its addresses has been registered before, it returns
     * the addresses corresponding to this instance uuid.
     * @param uuid instance uuid
     * @return the registered addresses corresponds to given instance uuid
     */
    @Nullable
    public LinkedAddresses linkedAddressesOf(@Nonnull UUID uuid) {
        if (uuid.equals(localUuid)) {
            return localAddresses;
        }
        Pair pair = uuidToAddresses.get(uuid);
        return pair != null ? pair.getAddresses() : null;
    }

    /**
     * If this instance uuid and its addresses has been registered before, it returns
     * the primary address corresponding to this instance uuid.
     * @param uuid instance uuid
     * @return the primary address for the instance corresponds to given uuid
     */
    @Nullable
    public Address getPrimaryAddress(@Nonnull UUID uuid) {
        if (uuid.equals(localUuid)) {
            return localAddresses.getPrimaryAddress();
        }
        LinkedAddresses linkedAddresses = linkedAddressesOf(uuid);
        return linkedAddresses != null ? linkedAddresses.getPrimaryAddress() : null;
    }

    public void reset() {
        addressToUuid.clear();
        uuidToAddresses.clear();
    }

    public void setLocalUuid(UUID newUuid) {
        localUuid = newUuid;
    }

    private void registerLocalAddresses(UUID thisUuid, AddressPicker addressPicker) {
        LinkedAddresses addresses =
                LinkedAddresses.getResolvedAddresses(addressPicker.getPublicAddress(EndpointQualifier.MEMBER));
        for (Map.Entry<EndpointQualifier, Address> addressEntry : addressPicker.getBindAddressMap().entrySet()) {
            addresses.addAllResolvedAddresses(addressPicker.getPublicAddress(addressEntry.getKey()));
            addresses.addAllResolvedAddresses(addressEntry.getValue());
            ServerSocketChannel serverSocketChannel = addressPicker.getServerSocketChannel(addressEntry.getKey());
            if (serverSocketChannel != null && serverSocketChannel.socket().getInetAddress()
                    .getHostAddress().equals("0.0.0.0")) {
                int port = addressEntry.getValue().getPort();
                try {
                    Collections.list(NetworkInterface.getNetworkInterfaces())
                            .forEach(networkInterface ->
                                    Collections.list(networkInterface.getInetAddresses())
                                            .forEach(inetAddress ->
                                                    addresses.addAllResolvedAddresses(new Address(inetAddress, port))));
                } catch (SocketException e) {
                    ignore(e);
                }
            }
        }
        if (logger.isFineEnabled()) {
            logger.fine(addresses + " are registered for the local member");
        }
        localUuid = thisUuid;
        localAddresses = addresses;
    }

    private static final class Pair {
        private final LinkedAddresses addresses;
        private final AtomicInteger registrationCount;

        private Pair(LinkedAddresses addresses, AtomicInteger connectionCount) {
            this.addresses = addresses;
            this.registrationCount = connectionCount;
        }

        public LinkedAddresses getAddresses() {
            return addresses;
        }

        public AtomicInteger getRegistrationCount() {
            return registrationCount;
        }
    }
}
