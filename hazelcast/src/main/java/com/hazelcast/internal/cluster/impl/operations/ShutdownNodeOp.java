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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;

import static com.hazelcast.internal.util.ThreadUtil.createThreadName;

public class ShutdownNodeOp extends AbstractClusterOperation implements AllowedDuringPassiveState {

    @Override
    public void run() {
        final ClusterServiceImpl clusterService = getService();
        final ILogger logger = getLogger();

        final ClusterState clusterState = clusterService.getClusterState();

        if (clusterState == ClusterState.PASSIVE) {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            if (nodeEngine.isRunning()) {
                logger.info("Shutting down node in cluster passive state. Requested by: " + getCallerAddress());
                new Thread(() -> {
                    Node node = nodeEngine.getNode();
                    node.hazelcastInstance.getLifecycleService().shutdown();
                }, createThreadName(nodeEngine.getHazelcastInstance().getName(), ".clusterShutdown")).start();
            } else {
                logger.info("Node is already shutting down. NodeState: " + nodeEngine.getNode().getState());
            }
        } else {
            logger.severe("Can not shut down node because cluster is in " + clusterState + " state. Requested by: "
                    + getCallerAddress());
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.SHUTDOWN_NODE;
    }
}
