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

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public class MembersUpdateOp extends AbstractClusterOperation {
    /** The master cluster clock time. */
    long masterTime = Clock.currentTimeMillis();
    PartitionRuntimeState partitionRuntimeState;
    /** The updated member info collection. */
    private List<MemberInfo> memberInfos;
    /** The UUID of the receiving member. */
    private UUID targetUuid;
    private boolean returnResponse;
    private int memberListVersion;

    public MembersUpdateOp() {
        memberInfos = emptyList();
    }

    public MembersUpdateOp(UUID targetUuid, MembersView membersView, long masterTime,
                           PartitionRuntimeState partitionRuntimeState, boolean returnResponse) {
        this.targetUuid = targetUuid;
        this.masterTime = masterTime;
        this.memberInfos = membersView.getMembers();
        this.returnResponse = returnResponse;
        this.partitionRuntimeState = partitionRuntimeState;
        this.memberListVersion = membersView.getVersion();
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl clusterService = getService();
        Address callerAddress = getConnectionEndpointOrThisAddress();
        UUID callerUuid = getCallerUuid();
        if (clusterService.updateMembers(getMembersView(), callerAddress, callerUuid, targetUuid)) {
            processPartitionState();
        }
    }

    final int getMemberListVersion() {
        return memberListVersion;
    }

    final MembersView getMembersView() {
        return new MembersView(getMemberListVersion(), unmodifiableList(memberInfos));
    }

    final UUID getTargetUuid() {
        return targetUuid;
    }

    final Address getConnectionEndpointOrThisAddress() {
        ClusterServiceImpl clusterService = getService();
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        Node node = nodeEngine.getNode();
        Connection conn = getConnection();
        return conn != null ? conn.getRemoteAddress() : node.getThisAddress();
    }

    final void processPartitionState() {
        if (partitionRuntimeState == null) {
            return;
        }

        partitionRuntimeState.setMaster(getCallerAddress());
        ClusterServiceImpl clusterService = getService();
        Node node = clusterService.getNodeEngine().getNode();
        node.partitionService.processPartitionRuntimeState(partitionRuntimeState);
    }

    @Override
    public final boolean returnsResponse() {
        return returnResponse;
    }

    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        targetUuid = UUIDSerializationUtil.readUUID(in);
        masterTime = in.readLong();
        memberInfos = readList(in);
        partitionRuntimeState = in.readObject();
        returnResponse = in.readBoolean();
    }

    @Override
    protected final void readInternal(ObjectDataInput in) throws IOException {
        readInternalImpl(in);
        memberListVersion = in.readInt();
    }

    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, targetUuid);
        out.writeLong(masterTime);
        writeList(memberInfos, out);
        out.writeObject(partitionRuntimeState);
        out.writeBoolean(returnResponse);
    }

    @Override
    protected final void writeInternal(ObjectDataOutput out) throws IOException {
        writeInternalImpl(out);
        out.writeInt(memberListVersion);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", targetUuid=").append(targetUuid);
        sb.append(", members=");
        for (MemberInfo address : memberInfos) {
            sb.append(address).append(' ');
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
    }

}

