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
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Sent by the master to all members to finalize the join operation from a joining/returning node.
 */
public class FinalizeJoinOp extends MembersUpdateOp {
    /** The operation to be executed on the target node after join completes, can be {@code null}. */
    private PostJoinOp postJoinOp;
    private String clusterId;
    private long clusterStartTime;
    private ClusterState clusterState;
    private Version clusterVersion;

    private transient boolean finalized;

    public FinalizeJoinOp() {
    }

    public FinalizeJoinOp(String targetUuid, MembersView members, PostJoinOp postJoinOp,
                          long masterTime, String clusterId, long clusterStartTime, ClusterState clusterState,
                          Version clusterVersion, PartitionRuntimeState partitionRuntimeState, boolean sendResponse) {
        super(targetUuid, members, masterTime, partitionRuntimeState, sendResponse);
        this.postJoinOp = postJoinOp;
        this.clusterId = clusterId;
        this.clusterStartTime = clusterStartTime;
        this.clusterState = clusterState;
        this.clusterVersion = clusterVersion;
    }

    @Override
    public void run() throws Exception {
        checkLocalMemberUuid();

        ClusterServiceImpl clusterService = getService();
        Address callerAddress = getConnectionEndpointOrThisAddress();
        String callerUuid = getCallerUuid();

        finalized = clusterService.finalizeJoin(getMembersView(), callerAddress, callerUuid,
                clusterId, clusterState, clusterVersion, clusterStartTime, masterTime);

        if (!finalized) {
            return;
        }

        processPartitionState();
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        if (!finalized) {
            return;
        }

        sendPostJoinOperations();
        runPostJoinOp();
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
        operationService.run(postJoinOp);
    }

    private void sendPostJoinOperations() {
        final ClusterServiceImpl clusterService = getService();
        final NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        // Post join operations must be lock free; means no locks at all;
        // no partition locks, no key-based locks, no service level locks!
        final Operation[] postJoinOperations = nodeEngine.getPostJoinOperations();

        if (postJoinOperations != null && postJoinOperations.length > 0) {
            final OperationService operationService = nodeEngine.getOperationService();
            final Collection<Member> members = clusterService.getMembers();

            for (Member member : members) {
                if (!member.localMember()) {
                    PostJoinOp operation = new PostJoinOp(postJoinOperations);
                    operationService.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, operation, member.getAddress());
                }
            }
        }
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        super.writeInternalImpl(out);

        boolean hasPJOp = postJoinOp != null;
        out.writeBoolean(hasPJOp);
        if (hasPJOp) {
            postJoinOp.writeData(out);
        }
        out.writeUTF(clusterId);
        out.writeLong(clusterStartTime);
        out.writeUTF(clusterState.toString());
        out.writeObject(clusterVersion);
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        super.readInternalImpl(in);

        boolean hasPJOp = in.readBoolean();
        if (hasPJOp) {
            postJoinOp = new PostJoinOp();
            postJoinOp.readData(in);
        }
        clusterId = in.readUTF();
        clusterStartTime = in.readLong();
        String stateName = in.readUTF();
        clusterState = ClusterState.valueOf(stateName);
        clusterVersion = in.readObject();
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

