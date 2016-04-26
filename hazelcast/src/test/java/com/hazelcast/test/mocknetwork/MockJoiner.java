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
                    lookupMasterAddress();

                    if (node.getThisAddress().equals(masterAddress)) {
                        node.setJoined();
                        node.setAsMaster();
                        break;
                    }

                    if (masterAddress != null) {
                        node.setMasterAddress(masterAddress);
                        clusterJoinManager.sendJoinRequest(masterAddress, true);
                    }

                    Thread.sleep(50);
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
            masterAddress = node.getThisAddress();
            return;
        }

        Address master;
        if (nodeEngine.getNode().isMaster()) {
            master = nodeEngine.getThisAddress();
        } else {
            master = nodeEngine.getMasterAddress();
        }

        if (master == null) {
            masterAddress = node.getThisAddress();
            return;
        }

        NodeEngineImpl masterNodeEngine = nodes.get(master);
        if (masterNodeEngine != null && masterNodeEngine.isRunning() && masterNodeEngine.getNode().joined()) {
            masterAddress = master;
        }
    }

    private NodeEngineImpl findAliveNodeEngine() {
        for (Address address : joinAddresses) {
            NodeEngineImpl nodeEngine = nodes.get(address);
            if (nodeEngine != null && nodeEngine.isRunning() && nodeEngine.getNode().joined()) {
                return nodeEngine;
            }
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
