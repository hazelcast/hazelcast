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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.datastructures.snapshot.KeyValueDataChunk;
import com.hazelcast.cp.internal.datastructures.snapshot.ValueDataChunk;
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings({"checkstyle:declarationorder", "checkstyle:classdataabstractioncoupling"})
public final class RaftDataSerializerHook extends RaftDataSerializerConstants implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case PRE_VOTE_REQUEST -> new PreVoteRequest();
            case PRE_VOTE_RESPONSE -> new PreVoteResponse();
            case VOTE_REQUEST -> new VoteRequest();
            case VOTE_RESPONSE -> new VoteResponse();
            case APPEND_REQUEST -> new AppendRequest();
            case APPEND_SUCCESS_RESPONSE -> new AppendSuccessResponse();
            case APPEND_FAILURE_RESPONSE -> new AppendFailureResponse();
            case LOG_ENTRY -> new LogEntry();
            case SNAPSHOT_ENTRY -> new SnapshotEntry();
            case SNAPSHOT_CHUNK -> new SnapshotChunk();
            case DATA_CHUNK_GROUP -> new DataChunkGroup();
            case KEY_VALUE_DATA_CHUNK -> new KeyValueDataChunk();
            case VALUE_DATA_CHUNK -> new ValueDataChunk();
            case INSTALL_SNAPSHOT_REQUEST -> new InstallSnapshotRequest();
            case INSTALL_SNAPSHOT_RESPONSE -> new InstallSnapshotResponse();
            case DESTROY_RAFT_GROUP_COMMAND -> new DestroyRaftGroupCmd();
            case UPDATE_RAFT_GROUP_MEMBERS_COMMAND -> new UpdateRaftGroupMembersCmd();
            case TRIGGER_LEADER_ELECTION -> new TriggerLeaderElection();
            default -> throw new IllegalArgumentException("Undefined type: " + typeId);
        };
    }
}
