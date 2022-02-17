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
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult.CANNOT_MERGE;
import static com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage.SplitBrainMergeCheckResult.REMOTE_NODE_SHOULD_MERGE;

/**
 * Validate whether clusters may merge to recover from a split brain, based on configuration &amp; cluster version.
 */
public class SplitBrainMergeValidationOp extends AbstractJoinOperation {

    private SplitBrainJoinMessage request;
    private SplitBrainJoinMessage response;

    private transient Member suspectedCaller;

    public SplitBrainMergeValidationOp() {
    }

    public SplitBrainMergeValidationOp(SplitBrainJoinMessage request) {
        this.request = request;
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();

        if (!preCheck(node)) {
            return;
        }

        if (!masterCheck()) {
            return;
        }

        if (request != null) {
            ILogger logger = getLogger();
            if (checkSplitBrainJoinMessage()) {
                response = node.createSplitBrainJoinMessage();
            }

            if (logger.isFineEnabled()) {
                logger.fine("Returning " + response + " to " + getCallerAddress());
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (suspectedCaller != null) {
            ClusterServiceImpl service = getService();
            // I am the master. I can remove the member directly
            String reason = "Removing " + suspectedCaller + ", since it thinks it's already split from this cluster "
                    + "and looking to merge.";
            service.suspectMember(suspectedCaller, reason, true);
        }
    }

    private boolean preCheck(Node node) {
        ILogger logger = getLogger();
        ClusterService clusterService = node.getClusterService();
        if (!clusterService.isJoined()) {
            logger.info("Ignoring join check from " + getCallerAddress()
                    + ", because this node is not joined to a cluster yet...");
            return false;
        }

        if (!node.isRunning()) {
            logger.info("Ignoring join check from " + getCallerAddress() + " because this node is not active...");
            return false;
        }

        final ClusterState clusterState = clusterService.getClusterState();
        if (!clusterState.isJoinAllowed()) {
            logger.info("Ignoring join check from " + getCallerAddress() + " because cluster is in "
                    + clusterState + " state...");
            return false;
        }

        return true;
    }

    private boolean masterCheck() {
        ILogger logger = getLogger();
        ClusterServiceImpl service = getService();

        if (service.isMaster()) {
            Member existingMember = service.getMembershipManager().getMember(request.getAddress(), request.getUuid());
            if (existingMember != null) {
                logger.info("Removing " + suspectedCaller + ", since it thinks it's already split from this cluster "
                        + "and looking to merge.");
                suspectedCaller = existingMember;
            }
            return true;
        } else {
            logger.info("Ignoring join check from " + getCallerAddress() + ", because this node is not master...");
            return false;
        }
    }

    private boolean checkSplitBrainJoinMessage() {
        ClusterServiceImpl service = getService();
        ILogger logger = getLogger();
        try {
            if (!service.getClusterJoinManager().validateJoinMessage(request)) {
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
                return false;
            }

            if (!service.getClusterVersion().equals(request.getClusterVersion())) {
                logger.info("Join check from " + getCallerAddress() + " failed validation due to incompatible version,"
                        + "remote cluster version is " + request.getClusterVersion() + ", this cluster is "
                        + service.getClusterVersion());
                return false;
            }

            SplitBrainMergeCheckResult result = service.getClusterJoinManager().shouldMerge(request);
            if (result == REMOTE_NODE_SHOULD_MERGE) {
                return service.getMembershipManager().verifySplitBrainMergeMemberListVersion(request);
            }

            return result != CANNOT_MERGE;
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Could not validate split-brain join message! -> " + e.getMessage());
            }
            return false;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        request = in.readObject();
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        out.writeObject(request);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.SPLIT_BRAIN_MERGE_VALIDATION;
    }
}

