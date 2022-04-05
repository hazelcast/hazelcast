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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;

/**
 * Replicates a {@link DestroyRaftGroupCmd} to a Raft group.
 */
public class DestroyRaftGroupOp extends RaftReplicateOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    public DestroyRaftGroupOp() {
    }

    public DestroyRaftGroupOp(CPGroupId groupId) {
        super(groupId);
    }

    @Override
    protected InternalCompletableFuture replicate(RaftNode raftNode) {
        return raftNode.replicate(new DestroyRaftGroupCmd());
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.DESTROY_RAFT_GROUP_OP;
    }
}
