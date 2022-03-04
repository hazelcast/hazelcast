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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static com.hazelcast.spi.properties.ClusterProperty.WAIT_SECONDS_BEFORE_JOIN;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DiscoveryJoiner
        extends TcpIpJoiner {

    private final DiscoveryService discoveryService;
    private final boolean usePublicAddress;
    private final IdleStrategy idleStrategy =
            new BackoffIdleStrategy(0, 0, MILLISECONDS.toNanos(10),
                    MILLISECONDS.toNanos(500));
    private final int maximumWaitingTimeBeforeJoinSeconds;


    public DiscoveryJoiner(Node node, DiscoveryService discoveryService, boolean usePublicAddress) {
        super(node);
        this.maximumWaitingTimeBeforeJoinSeconds = node.getProperties().getInteger(WAIT_SECONDS_BEFORE_JOIN);
        this.discoveryService = discoveryService;
        this.usePublicAddress = usePublicAddress;
    }

    @Override
    protected Collection<Address> getPossibleAddressesForInitialJoin() {
        long deadLine = System.nanoTime() + SECONDS.toNanos(maximumWaitingTimeBeforeJoinSeconds);
        for (int i = 0; System.nanoTime() < deadLine; i++) {
            Collection<Address> possibleAddresses = getPossibleAddresses();
            if (!possibleAddresses.isEmpty()) {
                return possibleAddresses;
            }
            idleStrategy.idle(i);
        }
        return Collections.emptyList();
    }

    @Override
    protected Collection<Address> getPossibleAddresses() {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");

        MemberImpl localMember = node.nodeEngine.getLocalMember();
        Set<Address> localAddresses = node.getLocalAddressRegistry().getLocalAddresses();

        Collection<Address> possibleMembers = new ArrayList<>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            Address discoveredAddress = usePublicAddress ? discoveryNode.getPublicAddress() : discoveryNode.getPrivateAddress();
            if (localAddresses.contains(discoveredAddress)) {
                if (!usePublicAddress && discoveryNode.getPublicAddress() != null) {
                    // enrich member with client public address
                    localMember.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                            publicAddress(localMember, discoveryNode));
                }
                continue;
            }
            possibleMembers.add(discoveredAddress);
        }
        return possibleMembers;
    }

    private Address publicAddress(MemberImpl localMember, DiscoveryNode discoveryNode) {
        if (localMember.getAddressMap().containsKey(EndpointQualifier.CLIENT)) {
            try {
                String publicHost = discoveryNode.getPublicAddress().getHost();
                int clientPort = localMember.getAddressMap().get(EndpointQualifier.CLIENT).getPort();
                return new Address(publicHost, clientPort);
            } catch (Exception e) {
                logger.fine(e);
                // Return default public address since public host with the (advanced network) client port cannot be resolved
            }
        }
        return discoveryNode.getPublicAddress();
    }
}
