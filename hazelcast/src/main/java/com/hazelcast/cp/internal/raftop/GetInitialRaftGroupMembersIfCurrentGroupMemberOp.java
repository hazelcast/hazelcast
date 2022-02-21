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

package com.hazelcast.cp.internal.raftop;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftNodeAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.internal.util.Preconditions.checkState;

/**
 * When a CP member is added to a Raft group, a new member list is committed to
 * the Raft group first. Then, new member list of the Raft group will be also
 * committed to the Raft group. After the new member list is committed to
 * the Raft group itself, the new member can receive append requests and it can
 * try to initialize its local {@link RaftNode} instance. In order to the
 * initialize itself, it can directly ask to the Raft group to verify if it is
 * a current member of the Raft group.
 * <p>
 * This operation is NOT committed to the Metadata group.
 * It is committed to the queried Raft group.
 */
public class GetInitialRaftGroupMembersIfCurrentGroupMemberOp extends RaftOp implements RaftNodeAware,
                                                                                        IndeterminateOperationStateAware,
                                                                                        IdentifiedDataSerializable {

    private RaftEndpoint endpoint;

    private RaftNode raftNode;

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp() {
    }

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp(RaftEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        checkState(raftNode != null, "RaftNode is not injected in " + groupId);
        Collection<RaftEndpoint> members = raftNode.getAppliedMembers();
        checkState(members.contains(endpoint), endpoint
                + " is not in the current committed member list: " + members + " of " + groupId);
        return new ArrayList<RaftEndpoint>(raftNode.getInitialMembers());
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP;
    }

    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
    }
}
