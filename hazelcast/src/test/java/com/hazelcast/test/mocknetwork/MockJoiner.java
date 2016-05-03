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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

class MockJoiner extends AbstractJoiner {

    private final Collection<Address> joinAddresses;
    private final ConcurrentMap<Address, NodeEngineImpl> nodes;
    private final Object joinerLock;

    private Address masterAddress;

    MockJoiner(Node node, Collection<Address> addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Object joinerLock) {
        super(node);
        this.joinAddresses = addresses;
        this.nodes = nodes;
        this.joinerLock = joinerLock;
    }

    public void doJoin() {
        synchronized (joinerLock) {

            ClusterJoinManager clusterJoinManager = node.clusterService.getClusterJoinManager();
            long joinStartTime = Clock.currentTimeMillis();
            long maxJoinMillis = getMaxJoinMillis();

            while (node.isRunning() && !node.joined()
                    && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
                try {
                    if (masterAddress == null) {
                        lookupMasterAddress();
                    }

                    if (node.getThisAddress().equals(masterAddress)) {
                        logger.fine("This node is found as master, no need to join.");
                        node.setJoined();
                        node.setAsMaster();
                        break;
                    }

                    if (masterAddress != null) {
                        logger.fine("Sending join request to master " + masterAddress);
                        node.setMasterAddress(masterAddress);
                        if (!clusterJoinManager.sendJoinRequest(masterAddress, true)) {
                            logger.fine("Could not send join request to " + masterAddress);
                            masterAddress = null;
                        }
                    }

                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (!node.joined()) {
                logger.severe("Node[" + node.getThisAddress() + "] should have been joined to " + node.getMasterAddress());
                node.shutdown(true);
            }
        }
    }

    private void lookupMasterAddress() {
        NodeEngineImpl nodeEngine = findAliveNodeEngine();
        if (nodeEngine == null) {
            logger.fine("Picking this node as master, no other running NodeEngine has been detected.");
            masterAddress = node.getThisAddress();
            return;
        }

        Address master;
        if (nodeEngine.getNode().isMaster()) {
            master = nodeEngine.getThisAddress();
            logger.fine("Found node itself is master: " + master);
        } else {
            master = nodeEngine.getMasterAddress();
            logger.fine("Found node " + nodeEngine.getThisAddress() + " knows master as: " + master);
        }

        if (master == null) {
            logger.fine("Picking this node as master, found NodeEngine has no master information.");
            masterAddress = node.getThisAddress();
            return;
        }

        NodeEngineImpl masterNodeEngine = nodes.get(master);
        if (masterNodeEngine == null) {
            logger.fine("NodeEngine for discovered master " + master + " is null.");
            return;
        }

        if (!masterNodeEngine.isRunning()) {
            logger.fine("NodeEngine for discovered master " + master + " is not running. -> "
                    + masterNodeEngine.getNode().getState());
            return;
        }

        if (!masterNodeEngine.getNode().joined()) {
            logger.fine("NodeEngine for discovered master " + master + " is not joined.");
            return;
        }

        logger.fine("Found possible master. Will try to connect to " + master);
        masterAddress = master;
    }

    private NodeEngineImpl findAliveNodeEngine() {
        logger.fine("Searching possible addresses for master " + joinAddresses);
        for (Address address : joinAddresses) {
            NodeEngineImpl nodeEngine = nodes.get(address);
            if (nodeEngine == null) {
                logger.fine("NodeEngine for " + address + " is null.");
                continue;
            }

            if (!nodeEngine.isRunning()) {
                logger.fine("NodeEngine for " + address + " is not running. -> " + nodeEngine.getNode().getState());
                continue;
            }

            if (!nodeEngine.getNode().joined()) {
                logger.fine("NodeEngine for " + address + " is not joined.");
                continue;
            }

            logger.fine("Found an alive node. Will ask master of " + address);
            return nodeEngine;
        }
        return null;
    }

    public void searchForOtherClusters() {
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
