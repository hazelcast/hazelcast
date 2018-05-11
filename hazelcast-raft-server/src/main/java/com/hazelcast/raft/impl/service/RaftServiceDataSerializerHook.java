package com.hazelcast.raft.impl.service;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.service.operation.NotifyTermChangeOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendFailureResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.InstallSnapshotOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteResponseOp;
import com.hazelcast.raft.impl.service.operation.metadata.AddRaftMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.CheckRemovedRaftMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.raft.impl.service.operation.metadata.CreateMetadataRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftNodeOp;
import com.hazelcast.raft.impl.service.operation.metadata.DestroyRaftNodesOp;
import com.hazelcast.raft.impl.service.operation.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveRaftMembersOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetMembershipChangeContextOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.SendActiveRaftMembersOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerExpandRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRebalanceRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRemoveRaftMemberOp;
import com.hazelcast.raft.impl.service.operation.snapshot.RestoreSnapshotOp;
import com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOp;
import com.hazelcast.raft.impl.service.proxy.DestroyRaftGroupOp;

public final class RaftServiceDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1002;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int GROUP_ID = 1;
    public static final int RAFT_GROUP_INFO = 2;
    public static final int PRE_VOTE_REQUEST_OP = 3;
    public static final int PRE_VOTE_RESPONSE_OP = 4;
    public static final int VOTE_REQUEST_OP = 5;
    public static final int VOTE_RESPONSE_OP = 6;
    public static final int APPEND_REQUEST_OP = 7;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 8;
    public static final int APPEND_FAILURE_RESPONSE_OP = 9;
    public static final int METADATA_SNAPSHOT = 10;
    public static final int INSTALL_SNAPSHOT_OP = 11;
    public static final int DEFAULT_RAFT_GROUP_REPLICATE_OP = 12;
    public static final int CREATE_RAFT_GROUP_OP = 13;
    public static final int TRIGGER_DESTROY_RAFT_GROUP_OP = 14;
    public static final int COMPLETE_DESTROY_RAFT_GROUPS_OP = 15;
    public static final int TRIGGER_REMOVE_RAFT_MEMBER_OP = 16;
    public static final int COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP = 17;
    public static final int MEMBERSHIP_CHANGE_REPLICATE_OP = 18;
    public static final int MEMBERSHIP_CHANGE_CTX = 19;
    public static final int DEFAULT_RAFT_GROUP_QUERY_OP = 20;
    public static final int CHECK_REMOVED_MEMBER_OP = 21;
    public static final int DESTROY_RAFT_NODES_OP = 22;
    public static final int GET_ACTIVE_RAFT_MEMBERS_OP = 23;
    public static final int GET_DESTROYING_RAFT_GROUP_IDS_OP = 24;
    public static final int GET_MEMBERSHIP_CHANGE_CONTEXT_OP = 25;
    public static final int GET_RAFT_GROUP_OP = 26;
    public static final int CREATE_RAFT_NODE_OP = 27;
    public static final int DESTROY_RAFT_GROUP_OP = 28;
    public static final int RESTORE_SNAPSHOT_OP = 29;
    public static final int NOTIFY_TERM_CHANGE_OP = 30;
    public static final int MEMBER = 31;
    public static final int SEND_ACTIVE_RAFT_MEMBERS_OP = 32;
    public static final int ADD_RAFT_MEMBER_OP = 33;
    public static final int TRIGGER_EXPAND_RAFT_GROUPS_OP = 34;
    public static final int TRIGGER_REBALANCE_RAFT_GROUPS_OP = 35;
    public static final int CREATE_METADATA_RAFT_GROUP_OP = 36;
    public static final int FORCE_DESTROY_RAFT_GROUP_OP = 37;
    public static final int GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP = 38;

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
                    case RAFT_GROUP_INFO:
                        return new RaftGroupInfo();
                    case PRE_VOTE_REQUEST_OP:
                        return new PreVoteRequestOp();
                    case PRE_VOTE_RESPONSE_OP:
                        return new PreVoteResponseOp();
                    case VOTE_REQUEST_OP:
                        return new VoteRequestOp();
                    case VOTE_RESPONSE_OP:
                        return new VoteResponseOp();
                    case APPEND_REQUEST_OP:
                        return new AppendRequestOp();
                    case APPEND_SUCCESS_RESPONSE_OP:
                        return new AppendSuccessResponseOp();
                    case APPEND_FAILURE_RESPONSE_OP:
                        return new AppendFailureResponseOp();
                    case METADATA_SNAPSHOT:
                        return new MetadataSnapshot();
                    case INSTALL_SNAPSHOT_OP:
                        return new InstallSnapshotOp();
                    case CREATE_RAFT_GROUP_OP:
                        return new CreateRaftGroupOp();
                    case DEFAULT_RAFT_GROUP_REPLICATE_OP:
                        return new DefaultRaftReplicateOp();
                    case TRIGGER_DESTROY_RAFT_GROUP_OP:
                        return new TriggerDestroyRaftGroupOp();
                    case COMPLETE_DESTROY_RAFT_GROUPS_OP:
                        return new CompleteDestroyRaftGroupsOp();
                    case TRIGGER_REMOVE_RAFT_MEMBER_OP:
                        return new TriggerRemoveRaftMemberOp();
                    case COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP:
                        return new CompleteRaftGroupMembershipChangesOp();
                    case MEMBERSHIP_CHANGE_REPLICATE_OP:
                        return new ChangeRaftGroupMembershipOp();
                    case MEMBERSHIP_CHANGE_CTX:
                        return new MembershipChangeContext();
                    case DEFAULT_RAFT_GROUP_QUERY_OP:
                        return new RaftQueryOp();
                    case CHECK_REMOVED_MEMBER_OP:
                        return new CheckRemovedRaftMemberOp();
                    case DESTROY_RAFT_NODES_OP:
                        return new DestroyRaftNodesOp();
                    case GET_ACTIVE_RAFT_MEMBERS_OP:
                        return new GetActiveRaftMembersOp();
                    case GET_DESTROYING_RAFT_GROUP_IDS_OP:
                        return new GetDestroyingRaftGroupIdsOp();
                    case GET_MEMBERSHIP_CHANGE_CONTEXT_OP:
                        return new GetMembershipChangeContextOp();
                    case GET_RAFT_GROUP_OP:
                        return new GetRaftGroupOp();
                    case CREATE_RAFT_NODE_OP:
                        return new CreateRaftNodeOp();
                    case DESTROY_RAFT_GROUP_OP:
                        return new DestroyRaftGroupOp();
                    case RESTORE_SNAPSHOT_OP:
                        return new RestoreSnapshotOp();
                    case NOTIFY_TERM_CHANGE_OP:
                        return new NotifyTermChangeOp();
                    case MEMBER:
                        return new RaftMemberImpl();
                    case SEND_ACTIVE_RAFT_MEMBERS_OP:
                        return new SendActiveRaftMembersOp();
                    case ADD_RAFT_MEMBER_OP:
                        return new AddRaftMemberOp();
                    case TRIGGER_EXPAND_RAFT_GROUPS_OP:
                        return new TriggerExpandRaftGroupsOp();
                    case TRIGGER_REBALANCE_RAFT_GROUPS_OP:
                        return new TriggerRebalanceRaftGroupsOp();
                    case CREATE_METADATA_RAFT_GROUP_OP:
                        return new CreateMetadataRaftGroupOp();
                    case FORCE_DESTROY_RAFT_GROUP_OP:
                        return new ForceDestroyRaftGroupOp();
                    case GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP:
                        return new GetInitialRaftGroupMembersIfCurrentGroupMemberOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
