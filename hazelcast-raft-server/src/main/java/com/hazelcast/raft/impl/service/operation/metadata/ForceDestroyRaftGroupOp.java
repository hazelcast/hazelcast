/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroup.RaftGroupStatus;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;

import java.io.IOException;

/**
 * If a Raft node loses its majority completely, its remaining members cannot leave the CP subsystem gracefully.
 * It is because we need to make a commit to a halted Raft group, and the system block since we won't be able to do it.
 * To recover from this failure case, we forcefully destroy a Raft group. Status of the Raft group will be directly
 * changed to {@link RaftGroupStatus#DESTROYED} without gracefully destroying {@link RaftNode} instances first.
 * <p/>
 * This operation is committed to the Metadata group.
 */
public class ForceDestroyRaftGroupOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private RaftGroupId targetGroupId;

    public ForceDestroyRaftGroupOp() {
    }

    public ForceDestroyRaftGroupOp(RaftGroupId targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        service.getMetadataGroupManager().forceDestroyRaftGroup(targetGroupId);
        return null;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.FORCE_DESTROY_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(targetGroupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        targetGroupId = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", targetGroupId=").append(targetGroupId);
    }
}
