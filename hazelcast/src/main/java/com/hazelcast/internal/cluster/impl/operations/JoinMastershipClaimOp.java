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

import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class JoinMastershipClaimOp extends AbstractJoinOperation {

    private transient boolean approvedAsMaster;

    @Override
    public void run() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();
        Joiner joiner = node.getJoiner();
        ClusterServiceImpl clusterService = node.getClusterService();
        final ILogger logger = node.getLogger(getClass().getName());
        if (joiner instanceof TcpIpJoiner) {
            TcpIpJoiner tcpIpJoiner = (TcpIpJoiner) joiner;
            final Address endpoint = getCallerAddress();
            final Address masterAddress = clusterService.getMasterAddress();
            approvedAsMaster = !tcpIpJoiner.isClaimingMastership() && !clusterService.isMaster()
                    && (masterAddress == null || masterAddress.equals(endpoint));
        } else {
            approvedAsMaster = false;
            logger.warning("This node requires MulticastJoin strategy!");
        }
        if (logger.isFineEnabled()) {
            logger.fine("Sending '" + approvedAsMaster + "' for master claim of node: " + getCallerAddress());
        }
    }

    @Override
    public Object getResponse() {
        return approvedAsMaster;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MASTER_CLAIM;
    }
}
