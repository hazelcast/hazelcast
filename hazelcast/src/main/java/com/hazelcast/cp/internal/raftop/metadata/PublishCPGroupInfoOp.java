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
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * CP group leaders send this operation to broadcast a change in their CP group view
 * to all other members of the Hazelcast cluster (AP and CP). Specifically, this
 * operation broadcasts which groups this member is leading along with the followers
 * of each group. All CP group leaders will send this operation after they become
 * leader, as well as periodically.
 * <p>
 * Please note that this operation is not a {@link RaftOp}, so it is not handled
 * via the Raft layer.
 */
public class PublishCPGroupInfoOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private CPMember cpLeader;
    // keyset represents led groups
    private Map<CPGroupId, Set<CPMember>> followerInfo;
    private Map<CPGroupId, Integer> groupToTerm;

    public PublishCPGroupInfoOp() {
    }

    public PublishCPGroupInfoOp(CPMember leader, Map<CPGroupId, Set<CPMember>> followerInfo,
                                Map<CPGroupId, Integer> groupToTerm) {
        this.cpLeader = leader;
        this.followerInfo = followerInfo;
        this.groupToTerm = groupToTerm;

        assert groupToTerm.keySet().containsAll(followerInfo.keySet());
    }

    @Override
    public void run() {
        RaftService service = getService();
        getLogger().finest("Received PublishCPGroupInfoOp from %s[%s]: leader: %s, followers: %s, terms: %s",
                getCallerUuid(), getCallerAddress(), cpLeader, followerInfo, groupToTerm);
        service.getGroupViewTracker().receiveUpdateForLedGroups(cpLeader, followerInfo, groupToTerm);
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
        return RaftServiceDataSerializerHook.PUBLISH_CP_GROUP_INFO_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(cpLeader);
        // follower info
        out.writeInt(followerInfo.size());
        for (Map.Entry<CPGroupId, Set<CPMember>> entry : followerInfo.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeInt(groupToTerm.get(entry.getKey()));
            out.writeInt(entry.getValue().size());
            for (CPMember cpMember : entry.getValue()) {
                out.writeObject(cpMember);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cpLeader = in.readObject();
        int groupsLen = in.readInt();
        // follower info
        followerInfo = new HashMap<>(groupsLen);
        groupToTerm = new HashMap<>(groupsLen);
        for (int i = 0; i < groupsLen; i++) {
            CPGroupId groupId = in.readObject();
            int term = in.readInt();
            int followersLen = in.readInt();
            Set<CPMember> followers = new HashSet<>(followersLen);
            for (int k = 0; k < followersLen; k++) {
                followers.add(in.readObject());
            }
            followerInfo.put(groupId, followers);
            groupToTerm.put(groupId, term);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", cpLeader=").append(cpLeader)
                .append(", followerInfo=").append(followerInfo)
                .append(", groupToTerm=").append(groupToTerm);
    }
}
