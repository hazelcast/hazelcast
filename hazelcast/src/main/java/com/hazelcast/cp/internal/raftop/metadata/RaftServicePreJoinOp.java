/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * If the CP discovery process is completed, new Hazelcast nodes
 * skip the discovery step.
 * <p>
 * This operation also shares the master's current view of CP
 * membership across the cluster.
 * <p>
 * Please note that this operation is not a {@link RaftOp},
 * so it is not handled via the Raft layer.
 */
public class RaftServicePreJoinOp extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState, Versioned {

    private boolean discoveryCompleted;

    private RaftGroupId metadataGroupId;
    private CPGroupsSnapshot cpGroupsSnapshot;

    public RaftServicePreJoinOp() {
    }

    public RaftServicePreJoinOp(boolean discoveryCompleted, RaftGroupId metadataGroupId,
                                CPGroupsSnapshot cpGroupsSnapshot) {
        this.discoveryCompleted = discoveryCompleted;
        this.metadataGroupId = metadataGroupId;
        this.cpGroupsSnapshot = cpGroupsSnapshot;
    }

    @Override
    public void run() {
        RaftService service = getService();
        MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
        metadataGroupManager.handleMetadataGroupId(metadataGroupId);
        if (discoveryCompleted) {
            metadataGroupManager.disableDiscovery();
        }
        service.receivePreJoinSnapshot(cpGroupsSnapshot);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.RAFT_PRE_JOIN_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(discoveryCompleted);
        out.writeObject(metadataGroupId);
        Map<CPGroupId, CPGroupsSnapshot.GroupInfo> allGroupInfo = cpGroupsSnapshot.getAllGroupInformation();
        out.writeInt(allGroupInfo.size());
        for (Map.Entry<CPGroupId, CPGroupsSnapshot.GroupInfo> entry : allGroupInfo.entrySet()) {
            out.writeObject(entry.getKey());
            CPGroupsSnapshot.GroupInfo groupInfo = entry.getValue();
            out.writeInt(groupInfo.term());
            out.writeObject(groupInfo.leader());
            int followersLen = groupInfo.followers().size();
            out.writeInt(followersLen);
            for (CPMember follower : groupInfo.followers()) {
                out.writeObject(follower);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        discoveryCompleted = in.readBoolean();
        metadataGroupId = in.readObject();

        int allGroupsLen = in.readInt();
        Map<CPGroupId, CPGroupsSnapshot.GroupInfo> allGroupInfo = new HashMap<>(allGroupsLen);
        for (int k = 0; k < allGroupsLen; k++) {
            CPGroupId groupId = in.readObject();
            int term = in.readInt();
            CPMember leader = in.readObject();
            int followersLen = in.readInt();
            Set<CPMember> followers = new HashSet<>(followersLen);
            for (int i = 0; i < followersLen; i++) {
                followers.add(in.readObject());
            }
            allGroupInfo.put(groupId, new CPGroupsSnapshot.GroupInfo(leader, followers, term));
        }
        cpGroupsSnapshot = new CPGroupsSnapshot(allGroupInfo);

    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", discoveryCompleted=").append(discoveryCompleted)
          .append(", metadataGroupId=").append(metadataGroupId)
          .append(", cpGroupsSnapshot=").append(cpGroupsSnapshot);
    }
}
