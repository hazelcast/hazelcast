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
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Sent by the master to all members to finalize the join operation from a joining/returning node.
 */
public class FinalizeJoinOp extends MembersUpdateOp {
    /**
     * Operations to be executed before node is marked as joined.
     * @see com.hazelcast.spi.PreJoinAwareService
     * @since 3.9
     */
    private OnJoinOp preJoinOp;
    /** The operation to be executed on the target node after join completes, can be {@code null}. */
    private OnJoinOp postJoinOp;
    private String clusterId;
    private long clusterStartTime;
    private ClusterState clusterState;
    private Version clusterVersion;

    private transient boolean finalized;

    public FinalizeJoinOp() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public FinalizeJoinOp(String targetUuid, MembersView members, OnJoinOp preJoinOp, OnJoinOp postJoinOp,
                          long masterTime, String clusterId, long clusterStartTime, ClusterState clusterState,
                          Version clusterVersion, PartitionRuntimeState partitionRuntimeState, boolean sendResponse) {
        super(targetUuid, members, masterTime, partitionRuntimeState, sendResponse);
        if (clusterVersion.isLessThan(V3_9)) {
            assert preJoinOp == null : "Cannot execute pre join operations when cluster version is " + clusterVersion;
        }
        this.preJoinOp = preJoinOp;
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

        preparePostOp(preJoinOp);
        finalized = clusterService.finalizeJoin(getMembersView(), callerAddress, callerUuid,
                clusterId, clusterState, clusterVersion, clusterStartTime, masterTime, preJoinOp);

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
        preparePostOp(postJoinOp);
        getNodeEngine().getOperationService().run(postJoinOp);
    }

    private void preparePostOp(Operation postOp) {
        if (postOp == null) {
            return;
        }

        ClusterServiceImpl clusterService = getService();
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        postOp.setNodeEngine(nodeEngine);
        OperationAccessor.setCallerAddress(postOp, getCallerAddress());
        OperationAccessor.setConnection(postOp, getConnection());
        postOp.setOperationResponseHandler(createEmptyResponseHandler());
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
                    OnJoinOp operation = new OnJoinOp(postJoinOperations);
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
        if (clusterVersion.isGreaterOrEqual(V3_9)) {
            out.writeObject(preJoinOp);
        }
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        super.readInternalImpl(in);

        boolean hasPostJoinOp = in.readBoolean();
        if (hasPostJoinOp) {
            postJoinOp = new OnJoinOp();
            postJoinOp.readData(in);
        }
        clusterId = in.readUTF();
        clusterStartTime = in.readLong();
        String stateName = in.readUTF();
        clusterState = ClusterState.valueOf(stateName);
        clusterVersion = in.readObject();
        if (clusterVersion.isGreaterOrEqual(V3_9)) {
            preJoinOp = in.readObject();
        }
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

