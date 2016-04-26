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
 */

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterClockImpl;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

public class FinalizeJoinOperation extends MemberInfoUpdateOperation implements JoinOperation {

    public static final int FINALIZE_JOIN_TIMEOUT_FACTOR = 5;
    public static final int FINALIZE_JOIN_MAX_TIMEOUT = 60;

    private static final int POST_JOIN_TRY_COUNT = 100;

    private PostJoinOperation postJoinOp;
    private PartitionRuntimeState partitionRuntimeState;
    private String clusterId;
    private long clusterStartTime;
    private ClusterState clusterState;

    public FinalizeJoinOperation() {
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp, long masterTime,
                                 String clusterId, long clusterStartTime, ClusterState clusterState,
                                 PartitionRuntimeState partitionRuntimeState) {
        super(members, masterTime, true);
        this.postJoinOp = postJoinOp;
        this.clusterId = clusterId;
        this.clusterStartTime = clusterStartTime;
        this.clusterState = clusterState;
        this.partitionRuntimeState = partitionRuntimeState;
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp, long masterTime,
                                 ClusterState clusterState, PartitionRuntimeState partitionRuntimeState, boolean sendResponse) {
        super(members, masterTime, sendResponse);
        this.postJoinOp = postJoinOp;
        this.clusterState = clusterState;
        this.partitionRuntimeState = partitionRuntimeState;
    }

    @Override
    public void run() throws Exception {
        if (!isValid()) {
            return;
        }

        final ClusterServiceImpl clusterService = getService();
        final NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        if (nodeEngine.getNode().joined()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Node is already joined... No need to finalize join...");
            }
            return;
        }

        initClusterStates(clusterService);

        processMemberUpdate();

        processPartitionState();

        sendPostJoinOperations();

        runPostJoinOp();
    }

    private void initClusterStates(ClusterServiceImpl clusterService) {
        clusterService.initialClusterState(clusterState);
        clusterService.setClusterId(clusterId);
        ClusterClockImpl clusterClock = clusterService.getClusterClock();
        clusterClock.setClusterStartTime(clusterStartTime);
        clusterClock.setMasterTime(masterTime);
    }

    private void processPartitionState() {
        if (partitionRuntimeState == null) {
            return;
        }

        partitionRuntimeState.setEndpoint(getCallerAddress());
        ClusterServiceImpl clusterService = getService();
        Node node = clusterService.getNodeEngine().getNode();
        node.partitionService.processPartitionRuntimeState(partitionRuntimeState);
    }

    private void runPostJoinOp() {
        if (postJoinOp == null) {
            return;
        }

        ClusterServiceImpl clusterService = getService();
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        InternalOperationService operationService = nodeEngine.getOperationService();

        postJoinOp.setNodeEngine(nodeEngine);
        OperationAccessor.setCallerAddress(postJoinOp, getCallerAddress());
        OperationAccessor.setConnection(postJoinOp, getConnection());
        postJoinOp.setOperationResponseHandler(createEmptyResponseHandler());
        operationService.runOperationOnCallingThread(postJoinOp);
    }

    private void sendPostJoinOperations() {
        final ClusterServiceImpl clusterService = getService();
        final NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        // Post join operations must be lock free; means no locks at all;
        // no partition locks, no key-based locks, no service level locks!
        final Operation[] postJoinOperations = nodeEngine.getPostJoinOperations();
        final OperationService operationService = nodeEngine.getOperationService();

        if (postJoinOperations != null && postJoinOperations.length > 0) {
            final Collection<Member> members = clusterService.getMembers();
            for (Member member : members) {
                if (!member.localMember()) {
                    PostJoinOperation operation = new PostJoinOperation(postJoinOperations);
                    operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                            operation, member.getAddress()).setTryCount(POST_JOIN_TRY_COUNT).invoke();
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        boolean hasPJOp = postJoinOp != null;
        out.writeBoolean(hasPJOp);
        if (hasPJOp) {
            postJoinOp.writeData(out);
        }
        out.writeUTF(clusterId);
        out.writeLong(clusterStartTime);
        out.writeUTF(clusterState.toString());
        out.writeObject(partitionRuntimeState);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean hasPJOp = in.readBoolean();
        if (hasPJOp) {
            postJoinOp = new PostJoinOperation();
            postJoinOp.readData(in);
        }
        clusterId = in.readUTF();
        clusterStartTime = in.readLong();
        String stateName = in.readUTF();
        clusterState = ClusterState.valueOf(stateName);
        partitionRuntimeState = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", postJoinOp=").append(postJoinOp);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.FINALIZE_JOIN;
    }
}

