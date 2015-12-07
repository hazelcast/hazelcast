/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.impl.AbstractJoiner;
import com.hazelcast.cluster.impl.ClusterJoinManager;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

class MockJoiner extends AbstractJoiner {

    private final CopyOnWriteArrayList<Address> joinAddresses;
    private final ConcurrentMap<Address, NodeEngineImpl> nodes;
    private final Object joinerLock;

    MockJoiner(Node node, CopyOnWriteArrayList<Address> addresses, ConcurrentMap<Address, NodeEngineImpl> nodes, Object joinerLock) {
        super(node);
        this.joinAddresses = addresses;
        this.nodes = nodes;
        this.joinerLock = joinerLock;
    }

    public void doJoin() {
        NodeEngineImpl nodeEngine = null;
        synchronized (joinerLock) {
            for (Address address : joinAddresses) {
                NodeEngineImpl ne = nodes.get(address);
                if (ne != null && ne.isRunning() && ne.getNode().joined()) {
                    nodeEngine = ne;
                    break;
                }
            }
            Address master = null;
            if (nodeEngine != null) {
                if (nodeEngine.getNode().isMaster()) {
                    master = nodeEngine.getThisAddress();
                } else {
                    master = nodeEngine.getMasterAddress();
                }
            }
            if (master == null) {
                master = node.getThisAddress();
            }
            node.setMasterAddress(master);
            if (node.getMasterAddress().equals(node.getThisAddress())) {
                node.setJoined();
                node.setAsMaster();
            } else {
                final ClusterJoinManager clusterJoinManager = node.clusterService.getClusterJoinManager();

                for (int i = 0; !node.joined() && node.isRunning() && i < 2000; i++) {
                    try {
                        clusterJoinManager.sendJoinRequest(node.getMasterAddress(), true);
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
