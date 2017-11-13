package com.hazelcast.raft.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
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
import com.hazelcast.raft.impl.operation.ApplyRaftGroupMembersOp;
import com.hazelcast.raft.impl.operation.NopEntryOp;
import com.hazelcast.raft.impl.operation.RestoreSnapshotOp;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;

public final class RaftDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1001;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int GROUP_ID = 1;
    public static final int ENDPOINT = 2;
    public static final int PRE_VOTE_REQUEST = 3;
    public static final int PRE_VOTE_RESPONSE = 4;
    public static final int VOTE_REQUEST = 5;
    public static final int VOTE_RESPONSE = 6;
    public static final int APPEND_REQUEST = 7;
    public static final int APPEND_SUCCESS_RESPONSE = 8;
    public static final int APPEND_FAILURE_RESPONSE = 9;
    public static final int LOG_ENTRY = 10;
    public static final int SNAPSHOT_ENTRY = 11;
    public static final int INSTALL_SNAPSHOT = 12;
    public static final int RESTORE_SNAPSHOT_OP = 13;
    public static final int TERMINATE_RAFT_GROUP_OP = 14;
    public static final int APPLY_RAFT_GROUP_MEMBERS_OP = 15;
    public static final int NOP_ENTRY_OP = 16;

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
                    case GROUP_ID:
                        return new RaftGroupIdImpl();
                    case ENDPOINT:
                        return new RaftEndpoint();
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
                    case RESTORE_SNAPSHOT_OP:
                        return new RestoreSnapshotOp();
                    case TERMINATE_RAFT_GROUP_OP:
                        return new TerminateRaftGroupOp();
                    case APPLY_RAFT_GROUP_MEMBERS_OP:
                        return new ApplyRaftGroupMembersOp();
                    case NOP_ENTRY_OP:
                        return new NopEntryOp();

                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
