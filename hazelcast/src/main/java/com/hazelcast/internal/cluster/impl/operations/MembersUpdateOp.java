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

import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public class MembersUpdateOp extends VersionedClusterOperation {
    /** The master cluster clock time. */
    long masterTime = Clock.currentTimeMillis();
    /** The updated member info collection. */
    private List<MemberInfo> memberInfos;
    /** The UUID of the receiving member. */
    private String targetUuid;
    private boolean returnResponse;
    private PartitionRuntimeState partitionRuntimeState;

    public MembersUpdateOp() {
        super(0);
        memberInfos = emptyList();
    }

    public MembersUpdateOp(String targetUuid, MembersView membersView, long masterTime,
                           PartitionRuntimeState partitionRuntimeState, boolean returnResponse) {
        super(membersView.getVersion());
        this.targetUuid = targetUuid;
        this.masterTime = masterTime;
        this.memberInfos = membersView.getMembers();
        this.returnResponse = returnResponse;
        this.partitionRuntimeState = partitionRuntimeState;
    }

    @Override
    public void run() throws Exception {
        checkLocalMemberUuid();

        ClusterServiceImpl clusterService = getService();
        Address callerAddress = getConnectionEndpointOrThisAddress();
        String callerUuid = getCallerUuid();
        if (clusterService.updateMembers(getMembersView(), callerAddress, callerUuid)) {
            processPartitionState();
        }
    }

    final MembersView getMembersView() {
        return new MembersView(getMemberListVersion(), unmodifiableList(memberInfos));
    }

    final Address getConnectionEndpointOrThisAddress() {
        ClusterServiceImpl clusterService = getService();
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        Node node = nodeEngine.getNode();
        Connection conn = getConnection();
        return conn != null ? conn.getEndPoint() : node.getThisAddress();
    }

    final void processPartitionState() {
        if (partitionRuntimeState == null) {
            return;
        }

        partitionRuntimeState.setEndpoint(getCallerAddress());
        ClusterServiceImpl clusterService = getService();
        Node node = clusterService.getNodeEngine().getNode();
        node.partitionService.processPartitionRuntimeState(partitionRuntimeState);
    }

    final void checkLocalMemberUuid() {
        ClusterServiceImpl clusterService = getService();
        if (!clusterService.getThisUuid().equals(targetUuid)) {
            String msg = "targetUuid: " + targetUuid + " is different than this node's uuid: " + clusterService.getThisUuid();
            throw new IllegalStateException(msg);
        }
    }

    @Override
    public final boolean returnsResponse() {
        return returnResponse;
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        targetUuid = in.readUTF();
        masterTime = in.readLong();
        int size = in.readInt();
        memberInfos = new ArrayList<MemberInfo>(size);
        while (size-- > 0) {
            MemberInfo memberInfo = new MemberInfo();
            memberInfo.readData(in);
            memberInfos.add(memberInfo);
        }

        partitionRuntimeState = in.readObject();
        returnResponse = in.readBoolean();
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        out.writeUTF(targetUuid);
        out.writeLong(masterTime);
        out.writeInt(memberInfos.size());
        for (MemberInfo memberInfo : memberInfos) {
            memberInfo.writeData(out);
        }
        out.writeObject(partitionRuntimeState);
        out.writeBoolean(returnResponse);
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
    public int getId() {
        return ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
    }

}

