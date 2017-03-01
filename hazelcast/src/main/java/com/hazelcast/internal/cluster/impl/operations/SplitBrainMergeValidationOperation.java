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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * Validate whether clusters may merge to recover from a split brain, based on configuration & cluster version.
 */
public class SplitBrainMergeValidationOperation extends AbstractJoinOperation {

    private SplitBrainJoinMessage request;
    private SplitBrainJoinMessage response;

    private transient boolean removeCaller;

    public SplitBrainMergeValidationOperation() {
    }

    public SplitBrainMergeValidationOperation(SplitBrainJoinMessage request) {
        this.request = request;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();

        if (!preCheck(node)) {
            return;
        }

        if (!masterCheck(node)) {
            return;
        }

        if (request != null) {
            ILogger logger = getLogger();
            try {
                if (service.getClusterJoinManager().validateJoinMessage(request)) {
                    // Validate other cluster's major.minor version is same as this cluster.
                    // This way we ensure that all nodes of both clusters will be able to operate normally
                    // in the unified cluster which will be at the same cluster version as the current subclusters.
                    // If we only validated node codebase versions of master nodes, then we might end up with a
                    // unified cluster but some members might be kicked out due to incompatibility. For example
                    // assuming a 3.8.0 cluster with 2 nodes at codebase versions 3.8.0 & 3.9.0 (master) and another
                    // cluster with 3x3.9.0 nodes at cluster version 3.9.0: if we only validated on master nodes' codebase
                    // version, we would find them to be compatible and let the smaller cluster merge towards the bigger one.
                    // However we would end up with a cluster at cluster version 3.9.0 (including 4x3.9.0 nodes) and the
                    // 3.8.0-codebase node would be kicked out of the cluster. To enable the kicked-out node to join again
                    // the cluster, the user would be forced to upgrade the member to 3.9.0 codebase version.
                    // The implicit change of cluster version ("sneaky upgrade") and the change in membership would be a
                    // surprise to users and may cause unexpected issues.
                    if (service.getClusterVersion().equals(request.getClusterVersion())) {
                        response = node.createSplitBrainJoinMessage();
                    } else {
                        logger.info("Join check from " + getCallerAddress() + " failed validation due to incompatible version,"
                                + "remote cluster version is " + request.getClusterVersion() + ", this cluster is "
                                + service.getClusterVersion());
                    }
                }
                if (logger.isFineEnabled()) {
                    logger.fine("Returning " + response + " to " + getCallerAddress());
                }
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.fine("Could not validate split-brain join message! -> " + e.getMessage());
                }
            }
        }
    }

    private boolean masterCheck(Node node) {
        ILogger logger = getLogger();
        Address caller = getCallerAddress();
        ClusterServiceImpl service = getService();

        if (node.isMaster()) {
            if (service.getMember(caller) != null) {
                logger.info("Removing " + caller + ", since it thinks it's already split from this cluster "
                        + "and looking to merge.");
                removeCaller = true;
            }
            return true;
        } else {
            // ping master to check if it's still valid
            service.getClusterHeartbeatManager().sendMasterConfirmation();
            logger.info("Ignoring join check from " + caller
                    + ", because this node is not master...");
            return false;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (removeCaller) {
            ClusterServiceImpl service = getService();
            Address caller = getCallerAddress();
            service.removeAddress(caller, "Removing " + caller + ", since it thinks it's already split from this cluster "
                    + "and looking to merge.");
        }
    }

    private boolean preCheck(Node node) {
        ILogger logger = getLogger();
        if (!node.joined()) {
            logger.info("Ignoring join check from " + getCallerAddress()
                    + ", because this node is not joined to a cluster yet...");
            return false;
        }

        if (!node.isRunning()) {
            logger.info("Ignoring join check from " + getCallerAddress() + ", because this node is not active...");
            return false;
        }

        final ClusterState clusterState = node.clusterService.getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            logger.info("Ignoring join check from " + getCallerAddress() + ", because cluster is in "
                    + clusterState + " state ...");
            return false;
        }

        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        request = new SplitBrainJoinMessage();
        request.readData(in);
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        request.writeData(out);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.SPLIT_BRAIN_MERGE_VALIDATION;
    }
}

