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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Sent by the master to all members to finalize the join operation from a joining/returning node.
 */
public class FinalizeJoinOp extends MembersUpdateOp implements TargetAware {
    /**
     * Operations to be executed before node is marked as joined.
     * @see PreJoinAwareService
     * @since 3.9
     */
    private OnJoinOp preJoinOp;
    /** The operation to be executed on the target node after join completes, can be {@code null}. */
    private OnJoinOp postJoinOp;
    private UUID clusterId;
    private long clusterStartTime;
    private ClusterState clusterState;
    private Version clusterVersion;
    private boolean deferPartitionProcessing;

    private transient boolean finalized;
    private transient Exception deserializationFailure;

    public FinalizeJoinOp() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public FinalizeJoinOp(UUID targetUuid, MembersView members, OnJoinOp preJoinOp, OnJoinOp postJoinOp,
                          long masterTime, UUID clusterId, long clusterStartTime, ClusterState clusterState,
                          Version clusterVersion, PartitionRuntimeState partitionRuntimeState,
                          boolean deferPartitionProcessing) {
        super(targetUuid, members, masterTime, partitionRuntimeState, true);
        this.preJoinOp = preJoinOp;
        this.postJoinOp = postJoinOp;
        this.clusterId = clusterId;
        this.clusterStartTime = clusterStartTime;
        this.clusterState = clusterState;
        this.clusterVersion = clusterVersion;
        this.deferPartitionProcessing = deferPartitionProcessing;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl clusterService = getService();
        Address callerAddress = getConnectionEndpointOrThisAddress();
        UUID callerUuid = getCallerUuid();
        UUID targetUuid = getTargetUuid();

        checkDeserializationFailure(clusterService);

        preparePostOp(preJoinOp);
        InternalHotRestartService hrService = getInternalHotRestartService();
        boolean hrServiceEnabled = hrService != null && hrService.isEnabled();
        if (hrServiceEnabled) {
            // notify hot restart before setting initial cluster state
            hrService.setRejoiningActiveCluster(deferPartitionProcessing);
        }
        finalized = clusterService.finalizeJoin(getMembersView(), callerAddress, callerUuid, targetUuid, clusterId, clusterState,
                clusterVersion, clusterStartTime, masterTime, preJoinOp);

        if (!finalized) {
            return;
        }

        if (deferPartitionProcessing && hrServiceEnabled && partitionRuntimeState != null) {
            partitionRuntimeState.setMaster(getCallerAddress());
            hrService.deferApplyPartitionState(partitionRuntimeState);
        } else {
            processPartitionState();
        }
    }

    private InternalHotRestartService getInternalHotRestartService() {
        return getNodeEngine().getServiceOrNull(InternalHotRestartService.SERVICE_NAME);
    }

    private void checkDeserializationFailure(ClusterServiceImpl clusterService) {
        if (deserializationFailure != null) {
            getLogger().severe("Node could not join cluster.", deserializationFailure);
            Node node = clusterService.getNodeEngine().getNode();
            node.shutdown(true);
            throw ExceptionUtil.rethrow(deserializationFailure);
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        if (!finalized) {
            return;
        }

        final boolean shouldExecutePostJoinOp = preparePostOp(postJoinOp);
        if (deferPartitionProcessing && getInternalHotRestartService() != null && getInternalHotRestartService().isEnabled()) {
            getInternalHotRestartService().deferPostJoinOps(postJoinOp);
            return;
        }

        sendPostJoinOperationsBackToMaster();
        if (shouldExecutePostJoinOp) {
            getNodeEngine().getOperationService().run(postJoinOp);
        }
    }

    private boolean preparePostOp(Operation postOp) {
        if (postOp == null) {
            return false;
        }

        ClusterServiceImpl clusterService = getService();
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        postOp.setNodeEngine(nodeEngine);
        OperationAccessor.setCallerAddress(postOp, getCallerAddress());
        OperationAccessor.setConnection(postOp, getConnection());
        postOp.setOperationResponseHandler(createEmptyResponseHandler());
        return true;
    }

    private void sendPostJoinOperationsBackToMaster() {
        final ClusterServiceImpl clusterService = getService();
        final NodeEngineImpl nodeEngine = clusterService.getNodeEngine();

        // Post join operations must be lock free; means no locks at all;
        // no partition locks, no key-based locks, no service level locks!
        Collection<Operation> postJoinOperations = nodeEngine.getPostJoinOperations();

        if (postJoinOperations != null && !postJoinOperations.isEmpty()) {
            final OperationService operationService = nodeEngine.getOperationService();

            // send post join operations to master and it will broadcast it to all members
            Address masterAddress = clusterService.getMasterAddress();
            OnJoinOp operation = new OnJoinOp(postJoinOperations);
            operationService.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, operation, masterAddress);
        }
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        super.writeInternalImpl(out);
        UUIDSerializationUtil.writeUUID(out, clusterId);
        out.writeLong(clusterStartTime);
        out.writeString(clusterState.toString());
        out.writeObject(clusterVersion);
        out.writeObject(preJoinOp);
        out.writeObject(postJoinOp);
        out.writeBoolean(deferPartitionProcessing);
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        super.readInternalImpl(in);
        clusterId = UUIDSerializationUtil.readUUID(in);
        clusterStartTime = in.readLong();
        String stateName = in.readString();
        clusterState = ClusterState.valueOf(stateName);
        clusterVersion = in.readObject();
        preJoinOp = readOnJoinOp(in);
        postJoinOp = readOnJoinOp(in);
        deferPartitionProcessing = in.readBoolean();
    }

    private OnJoinOp readOnJoinOp(ObjectDataInput in) {
        if (deserializationFailure != null) {
            // return immediately, do not attempt to read further
            return null;
        }
        try {
            return in.readObject();
        } catch (Exception e) {
            deserializationFailure = e;
            return null;
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", postJoinOp=").append(postJoinOp);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.FINALIZE_JOIN;
    }

    @Override
    public void setTarget(Address address) {
        if (preJoinOp != null) {
            preJoinOp.setTarget(address);
        }
        if (postJoinOp != null) {
            postJoinOp.setTarget(address);
        }
    }
}

