/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class MemberInfoUpdateOperation extends AbstractClusterOperation implements JoinOperation, IdentifiedDataSerializable {

    protected String targetUuid;
    protected Collection<MemberInfo> memberInfos;
    protected long masterTime = Clock.currentTimeMillis();
    protected PartitionRuntimeState partitionRuntimeState;
    protected boolean sendResponse;

    public MemberInfoUpdateOperation() {
        memberInfos = new ArrayList<MemberInfo>();
    }

    public MemberInfoUpdateOperation(String targetUuid, Collection<MemberInfo> memberInfos, long masterTime,
                                     PartitionRuntimeState partitionRuntimeState, boolean sendResponse) {
        this.targetUuid = targetUuid;
        this.masterTime = masterTime;
        this.memberInfos = memberInfos;
        this.sendResponse = sendResponse;
        this.partitionRuntimeState = partitionRuntimeState;
    }

    @Override
    public void run() throws Exception {
        checkLocalMemberUuid();

        ClusterServiceImpl clusterService = getService();
        Address callerAddress = getConnectionEndpointOrThisAddress();
        if (clusterService.updateMembers(memberInfos, callerAddress)) {
            processPartitionState();
        }
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
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        Node node = nodeEngine.getNode();
        if (!node.getThisUuid().equals(targetUuid)) {
            String msg = "targetUuid: " + targetUuid + " is different than this node's uuid: " + node.getThisUuid();
            throw new IllegalStateException(msg);
        }
    }

    @Override
    public final boolean returnsResponse() {
        return sendResponse;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
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
        sendResponse = in.readBoolean();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(targetUuid);
        out.writeLong(masterTime);
        out.writeInt(memberInfos.size());
        for (MemberInfo memberInfo : memberInfos) {
            memberInfo.writeData(out);
        }
        out.writeObject(partitionRuntimeState);
        out.writeBoolean(sendResponse);
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

