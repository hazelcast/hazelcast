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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.AbstractJoiner;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult;
import com.hazelcast.internal.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;

import static com.hazelcast.test.mocknetwork.MockClusterProperty.MOCK_JOIN_PORT_TRY_COUNT;
import static com.hazelcast.test.mocknetwork.MockClusterProperty.MOCK_JOIN_SHOULD_ISOLATE_CLUSTERS;

class MockJoiner extends AbstractJoiner {

    private static final long JOIN_ADDRESS_TIMEOUT_IN_MILLIS = 5000;

    // blacklisted addresses
    private final Set<Address> blacklist;
    private final TestNodeRegistry registry;
    private final boolean shouldIsolateClusters;
    private final int maxTryCount;

    MockJoiner(Node node, TestNodeRegistry registry, Set<Address> initiallyBlockedAddresses) {
        super(node);
        this.registry = registry;
        this.blacklist = new CopyOnWriteArraySet<>(initiallyBlockedAddresses);

        shouldIsolateClusters = node.getProperties().getBoolean(MOCK_JOIN_SHOULD_ISOLATE_CLUSTERS);
        maxTryCount = node.getProperties().getInteger(MOCK_JOIN_PORT_TRY_COUNT);
    }

    public void doJoin() {
        registry.registerNode(node);

        final long joinStartTime = Clock.currentTimeMillis();
        final long maxJoinMillis = getMaxJoinMillis();

        Address previousJoinAddress = null;
        long joinAddressTimeout = 0;
        while (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis) {
            synchronized (registry) {
                Address joinAddress = getJoinAddress();
                verifyInvariant(joinAddress != null, "joinAddress should not be null");

                if (!joinAddress.equals(previousJoinAddress)) {
                    previousJoinAddress = joinAddress;
                    joinAddressTimeout = Clock.currentTimeMillis() + JOIN_ADDRESS_TIMEOUT_IN_MILLIS;
                }

                if (node.getThisAddress().equals(joinAddress)) {
                    logger.fine("This node is found as master, no need to join.");
                    clusterJoinManager.setThisMemberAsMaster();
                    break;
                }

                logger.fine("Sending join request to " + joinAddress);
                if (!clusterJoinManager.sendJoinRequest(joinAddress)) {
                    logger.fine("Could not send join request to " + joinAddress);
                    clusterService.setMasterAddressToJoin(null);
                }

                if (Clock.currentTimeMillis() > joinAddressTimeout) {
                    logger.warning("Resetting master address because join address timeout");
                    previousJoinAddress = null;
                    joinAddressTimeout = 0;
                    clusterService.setMasterAddressToJoin(null);
                }
            }
            if (!shouldRetry()) {
                break;
            }
            try {
                clusterService.blockOnJoin(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private Address getJoinAddress() {
        final Address targetAddress = getTargetAddress();
        if (targetAddress != null) {
            return targetAddress;
        }
        Address joinAddress = node.getMasterAddress();
        logger.fine("Known master address is: " + joinAddress);
        if (joinAddress == null) {
            joinAddress = lookupJoinAddress();
            if (!node.getThisAddress().equals(joinAddress)) {
                clusterJoinManager.sendMasterQuestion(joinAddress);
            }
        }
        return joinAddress;
    }

    private Address lookupJoinAddress() {
        Node foundNode = findAliveNode();
        if (foundNode == null) {
            logger.fine("Picking this node as master, no other running node has been detected.");
            return node.getThisAddress();
        }

        logger.fine("Found alive node. Will try to connect to " + foundNode.getThisAddress());
        return foundNode.getThisAddress();
    }

    private Node findAliveNode() {
        Collection<Address> joinAddresses = registry.getJoinAddresses();
        logger.fine("Searching possible addresses for master " + joinAddresses);
        for (Address address : joinAddresses) {
            Node foundNode = registry.getNode(address);
            if (foundNode == null) {
                logger.fine("Node for " + address + " is null.");
                continue;
            }

            verifyInvariant(address.equals(foundNode.getThisAddress()), "The address should be equal to the one in the found node");

            if (foundNode.getThisAddress().equals(node.getThisAddress())) {
                continue;
            }

            if (!foundNode.isRunning()) {
                logger.fine("Node for " + address + " is not running. -> " + foundNode.getState());
                continue;
            }

            if (!foundNode.getClusterService().isJoined()) {
                logger.fine("Node for " + address + " is not joined yet.");
                continue;
            }

            if (isBlacklisted(address)) {
                logger.fine("Node for " + address + " is blacklisted and should not be joined.");
                continue;
            }

            if (shouldIsolateClusters && isMemberOfIsolatedCluster(foundNode)) {
                String message = "Node for " + address + " is outside of the join range and should not be joined.";
                boolean suspicious = isSuspiciousIsolation(foundNode);
                logger.log(suspicious ? Level.WARNING : Level.FINE, message);
                continue;
            }

            logger.fine("Found an alive node. Will ask master of " + address);
            return foundNode;
        }
        return null;
    }

    private boolean isSuspiciousIsolation(Node node) {
        int thisPort = this.node.getConfig().getNetworkConfig().getPort();
        int thatPort = node.getConfig().getNetworkConfig().getPort();
        return thisPort == thatPort;
    }

    private boolean isMemberOfIsolatedCluster(Node node) {
        int rangeStartPort = this.node.getConfig().getNetworkConfig().getPort();
        int foundNodePort = node.getThisAddress().getPort();
        return rangeStartPort + maxTryCount < foundNodePort
                || rangeStartPort > foundNodePort;
    }

    public void searchForOtherClusters() {
        Collection<Address> possibleAddresses = new ArrayList<Address>(registry.getJoinAddresses());
        possibleAddresses.remove(node.getThisAddress());
        possibleAddresses.removeAll(node.getClusterService().getMemberAddresses());
        SplitBrainJoinMessage request = node.createSplitBrainJoinMessage();
        for (Address address : possibleAddresses) {
            SplitBrainMergeCheckResult result = sendSplitBrainJoinMessageAndCheckResponse(address, request);
            if (result == SplitBrainMergeCheckResult.LOCAL_NODE_SHOULD_MERGE) {
                startClusterMerge(address, request.getMemberListVersion());
                return;
            }
        }
    }

    @Override
    public String getType() {
        return "mock";
    }

    @Override
    public String toString() {
        return "MockJoiner";
    }

    @Override
    public void blacklist(Address address, boolean permanent) {
        // blacklist is always temporary in MockJoiner
        blacklist.add(address);
    }

    @Override
    public boolean unblacklist(Address address) {
        return blacklist.remove(address);
    }

    @Override
    public boolean isBlacklisted(Address address) {
        return blacklist.contains(address);
    }

    private static void verifyInvariant(boolean check, String msg) {
        if (!check) {
            throw new AssertionError(msg);
        }
    }
}
