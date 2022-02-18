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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * If a Raft node loses its majority completely, its remaining members cannot
 * leave CP Subsystem gracefully. It is because we need to make a commit to
 * a halted Raft group, and the system block since we won't be able to do it.
 * To recover from this failure case, we forcefully destroy a Raft group.
 * Status of the Raft group will be directly changed to
 * {@link CPGroupStatus#DESTROYED} without gracefully destroying
 * {@link RaftNode} instances first.
 * <p>
 * This operation is committed to the Metadata group.
 */
public class ForceDestroyRaftGroupOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                            IdentifiedDataSerializable {

    private String groupName;

    public ForceDestroyRaftGroupOp() {
    }

    public ForceDestroyRaftGroupOp(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        metadataGroupManager.forceDestroyRaftGroup(groupName);
        return null;
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
        return RaftServiceDataSerializerHook.FORCE_DESTROY_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(groupName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupName=").append(groupName);
    }
}
