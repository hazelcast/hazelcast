package com.hazelcast.raft.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.SnapshotEntry;

public final class RaftDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1001;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int PRE_VOTE_REQUEST = 1;
    public static final int PRE_VOTE_RESPONSE = 2;
    public static final int VOTE_REQUEST = 3;
    public static final int VOTE_RESPONSE = 4;
    public static final int APPEND_REQUEST = 5;
    public static final int APPEND_SUCCESS_RESPONSE = 6;
    public static final int APPEND_FAILURE_RESPONSE = 7;
    public static final int LOG_ENTRY = 8;
    public static final int SNAPSHOT_ENTRY = 9;
    public static final int INSTALL_SNAPSHOT = 10;
    public static final int DESTROY_RAFT_GROUP_COMMAND = 11;
    public static final int APPLY_RAFT_GROUP_MEMBERS_COMMAND = 12;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case PRE_VOTE_REQUEST:
                        return new PreVoteRequest();
                    case PRE_VOTE_RESPONSE:
                        return new PreVoteResponse();
                    case VOTE_REQUEST:
                        return new VoteRequest();
                    case VOTE_RESPONSE:
                        return new VoteResponse();
                    case APPEND_REQUEST:
                        return new AppendRequest();
                    case APPEND_SUCCESS_RESPONSE:
                        return new AppendSuccessResponse();
                    case APPEND_FAILURE_RESPONSE:
                        return new AppendFailureResponse();
                    case LOG_ENTRY:
                        return new LogEntry();
                    case SNAPSHOT_ENTRY:
                        return new SnapshotEntry();
                    case INSTALL_SNAPSHOT:
                        return new InstallSnapshot();
                    case DESTROY_RAFT_GROUP_COMMAND:
                        return new DestroyRaftGroupCmd();
                    case APPLY_RAFT_GROUP_MEMBERS_COMMAND:
                        return new ApplyRaftGroupMembersCmd();

                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
