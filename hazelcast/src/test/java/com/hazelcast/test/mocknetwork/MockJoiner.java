/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.test.mocknetwork;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.AbstractJoiner;
import com.hazelcast.internal.cluster.impl.ClusterJoinManager;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;

class MockJoiner extends AbstractJoiner {

    private final TestNodeRegistry registry;

    MockJoiner(Node node, TestNodeRegistry registry) {
        super(node);
        this.registry = registry;
    }

    private static void verifyInvariant(boolean check, String msg) {
        if (!check) throw new AssertionError(msg);
    }

    public void doJoin() {
        synchronized (registry) {
            registry.registerNode(node);

            ClusterJoinManager clusterJoinManager = node.clusterService.getClusterJoinManager();
            final long joinStartTime = Clock.currentTimeMillis();
            final long maxJoinMillis = getMaxJoinMillis();

            while (node.isRunning() && !node.joined() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
                try {
                    Address joinAddress = getJoinAddress();
                    verifyInvariant(joinAddress != null, "joinAddress should not be null");

                    if (node.getThisAddress().equals(joinAddress)) {
                        logger.fine("This node is found as master, no need to join.");
                        node.setJoined();
                        node.setAsMaster();
                        break;
                    }

                    logger.fine("Sending join request to " + joinAddress);
                    if (!clusterJoinManager.sendJoinRequest(joinAddress, true)) {
                        logger.fine("Could not send join request to " + joinAddress);
                        node.setMasterAddress(null);
                    }

                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }

        final boolean joined = node.joined();
        if (!joined) {
            node.shutdown(true);
        }
        verifyInvariant(joined, node.getThisAddress() + " should have been joined to " + node.getMasterAddress());
    }

    private Address getJoinAddress() {
        Address joinAddress = node.getMasterAddress();
        logger.fine("Known master address is: " + joinAddress);
        if (joinAddress == null) {
            joinAddress = lookupJoinAddress();
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

            if (!foundNode.joined()) {
                logger.fine("Node for " + address + " is not joined yet.");
                continue;
            }

            logger.fine("Found an alive node. Will ask master of " + address);
            return foundNode;
        }
        return null;
    }

    public void searchForOtherClusters() {
        Collection<Address> possibleAddresses = new ArrayList<Address>(registry.getJoinAddresses());
        possibleAddresses.remove(node.getThisAddress());
        possibleAddresses.removeAll(node.getClusterService().getMemberAddresses());
        for (Address address : possibleAddresses) {
            JoinMessage response = sendSplitBrainJoinMessage(address);
            if (shouldMerge(response)) {
                startClusterMerge(address);
            }
        }
    }

    @Override
    public String getType() {
        return "mock";
    }

    public String toString() {
        return "MockJoiner";
    }

    @Override
    public void blacklist(Address address, boolean permanent) {
    }

    @Override
    public boolean unblacklist(Address address) {
        return false;
    }

    @Override
    public boolean isBlacklisted(Address address) {
        return false;
    }
}
