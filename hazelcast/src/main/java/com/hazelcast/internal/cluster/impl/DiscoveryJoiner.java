/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.TcpIpJoiner;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Integer.getInteger;

public class DiscoveryJoiner
        extends TcpIpJoiner {
    private static final int DEFAULT_MAXIMUM_WAITING_FOR_INITIAL_MEMBERS_SECONDS = 5;
    private static final String WAITING_FOR_INITIAL_MEMBERS_SECONDS_PROP = "hazelcast.discovery.initial.join.seconds";

    private static final int MAXIMUM_WAITING_FOR_INITIAL_MEMBERS_SECONDS =
            getInteger(WAITING_FOR_INITIAL_MEMBERS_SECONDS_PROP, DEFAULT_MAXIMUM_WAITING_FOR_INITIAL_MEMBERS_SECONDS);

    private final DiscoveryService discoveryService;
    private final boolean usePublicAddress;

    public DiscoveryJoiner(Node node, DiscoveryService discoveryService, boolean usePublicAddress) {
        super(node);
        this.discoveryService = discoveryService;
        this.usePublicAddress = usePublicAddress;
    }

    @Override
    protected Collection<Address> getPossibleAddressesForInitialJoin() {
        Collection<Address> possibleAddresses;
        long deadLine = System.nanoTime() + TimeUnit.SECONDS.toNanos(MAXIMUM_WAITING_FOR_INITIAL_MEMBERS_SECONDS);
        do {
            possibleAddresses = getPossibleAddresses();
        } while (possibleAddresses.isEmpty() && System.nanoTime() < deadLine);
        return possibleAddresses;
    }

    @Override
    protected Collection<Address> getPossibleAddresses() {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");

        MemberImpl localMember = node.nodeEngine.getLocalMember();
        Address localAddress = localMember.getAddress();

        Collection<Address> possibleMembers = new ArrayList<Address>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            Address discoveredAddress = usePublicAddress ? discoveryNode.getPublicAddress() : discoveryNode.getPrivateAddress();
            if (localAddress.equals(discoveredAddress)) {
                continue;
            }
            possibleMembers.add(discoveredAddress);
        }
        return possibleMembers;
    }
}
