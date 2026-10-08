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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import com.hazelcast.cp.internal.operation.ChangeRaftGroupMembershipOp;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.cp.internal.operation.DestroyRaftGroupOp;
import com.hazelcast.cp.internal.operation.GetCPObjectInfosOp;
import com.hazelcast.cp.internal.operation.GetLeadedGroupsOp;
import com.hazelcast.cp.internal.operation.RaftQueryOp;
import com.hazelcast.cp.internal.operation.ResetCPMemberOp;
import com.hazelcast.cp.internal.operation.TransferLeadershipOp;
import com.hazelcast.cp.internal.operation.TriggerLeadershipRebalanceOp;
import com.hazelcast.cp.internal.operation.integration.AppendFailureResponseOp;
import com.hazelcast.cp.internal.operation.integration.AppendRequestOp;
import com.hazelcast.cp.internal.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.cp.internal.operation.integration.InstallSnapshotRequestOp;
import com.hazelcast.cp.internal.operation.integration.InstallSnapshotResponseOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteResponseOp;
import com.hazelcast.cp.internal.operation.integration.TriggerLeaderElectionOp;
import com.hazelcast.cp.internal.operation.integration.VoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.VoteResponseOp;
import com.hazelcast.cp.internal.raftop.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.cp.internal.raftop.NotifyTermChangeOp;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupByNameOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeScheduleOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.InitMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.PublishActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.PublishCPGroupInfoOp;
import com.hazelcast.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import com.hazelcast.cp.internal.raftop.metadata.RemoveCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.TerminateRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings({"checkstyle:declarationorder", "ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public final class RaftServiceDataSerializerHook extends RaftServiceSerializerConstants implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    @SuppressWarnings({"MethodLength", "CyclomaticComplexity"})
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case GROUP_ID -> new RaftGroupId();
            case RAFT_GROUP_INFO -> new CPGroupInfo();
            case PRE_VOTE_REQUEST_OP -> new PreVoteRequestOp();
            case PRE_VOTE_RESPONSE_OP -> new PreVoteResponseOp();
            case VOTE_REQUEST_OP -> new VoteRequestOp();
            case VOTE_RESPONSE_OP -> new VoteResponseOp();
            case APPEND_REQUEST_OP -> new AppendRequestOp();
            case APPEND_SUCCESS_RESPONSE_OP -> new AppendSuccessResponseOp();
            case APPEND_FAILURE_RESPONSE_OP -> new AppendFailureResponseOp();
            case METADATA_RAFT_GROUP_SNAPSHOT -> new MetadataRaftGroupSnapshot();
            case INSTALL_SNAPSHOT_REQUEST_OP -> new InstallSnapshotRequestOp();
            case INSTALL_SNAPSHOT_RESPONSE_OP -> new InstallSnapshotResponseOp();
            case CREATE_RAFT_GROUP_OP -> new CreateRaftGroupOp();
            case DEFAULT_RAFT_GROUP_REPLICATE_OP -> new DefaultRaftReplicateOp();
            case TRIGGER_DESTROY_RAFT_GROUP_OP -> new TriggerDestroyRaftGroupOp();
            case COMPLETE_DESTROY_RAFT_GROUPS_OP -> new CompleteDestroyRaftGroupsOp();
            case REMOVE_CP_MEMBER_OP -> new RemoveCPMemberOp();
            case COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP -> new CompleteRaftGroupMembershipChangesOp();
            case MEMBERSHIP_CHANGE_REPLICATE_OP -> new ChangeRaftGroupMembershipOp();
            case MEMBERSHIP_CHANGE_SCHEDULE -> new MembershipChangeSchedule();
            case DEFAULT_RAFT_GROUP_QUERY_OP -> new RaftQueryOp();
            case TERMINATE_RAFT_NODES_OP -> new TerminateRaftNodesOp();
            case GET_ACTIVE_CP_MEMBERS_OP -> new GetActiveCPMembersOp();
            case GET_DESTROYING_RAFT_GROUP_IDS_OP -> new GetDestroyingRaftGroupIdsOp();
            case GET_MEMBERSHIP_CHANGE_SCHEDULE_OP -> new GetMembershipChangeScheduleOp();
            case GET_RAFT_GROUP_OP -> new GetRaftGroupOp();
            case GET_ACTIVE_RAFT_GROUP_BY_NAME_OP -> new GetActiveRaftGroupByNameOp();
            case CREATE_RAFT_NODE_OP -> new CreateRaftNodeOp();
            case DESTROY_RAFT_GROUP_OP -> new DestroyRaftGroupOp();
            case RESTORE_SNAPSHOT_OP -> new RestoreSnapshotOp();
            case NOTIFY_TERM_CHANGE_OP -> new NotifyTermChangeOp();
            case CP_MEMBER -> new CPMemberInfo();
            case PUBLISH_ACTIVE_CP_MEMBERS_OP -> new PublishActiveCPMembersOp();
            case ADD_CP_MEMBER_OP -> new AddCPMemberOp();
            case INIT_METADATA_RAFT_GROUP_OP -> new InitMetadataRaftGroupOp();
            case FORCE_DESTROY_RAFT_GROUP_OP -> new ForceDestroyRaftGroupOp();
            case GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP ->
                    new GetInitialRaftGroupMembersIfCurrentGroupMemberOp();
            case GET_RAFT_GROUP_IDS_OP -> new GetRaftGroupIdsOp();
            case GET_ACTIVE_RAFT_GROUP_IDS_OP -> new GetActiveRaftGroupIdsOp();
            case RAFT_PRE_JOIN_OP -> new RaftServicePreJoinOp();
            case RESET_CP_MEMBER_OP -> new ResetCPMemberOp();
            case GROUP_MEMBERSHIP_CHANGE -> new CPGroupMembershipChange();
            case CP_ENDPOINT -> new RaftEndpointImpl();
            case CP_GROUP_SUMMARY -> new CPGroupSummary();
            case GET_LEADED_GROUPS -> new GetLeadedGroupsOp();
            case TRANSFER_LEADERSHIP_OP -> new TransferLeadershipOp();
            case TRIGGER_LEADER_ELECTION_OP -> new TriggerLeaderElectionOp();
            case GET_CP_OBJECT_INFOS_OP -> new GetCPObjectInfosOp();
            case PUBLISH_CP_GROUP_INFO_OP -> new PublishCPGroupInfoOp();
            case TRIGGER_LEADERSHIP_REBALANCE_OP -> new TriggerLeadershipRebalanceOp();
            default -> throw new IllegalArgumentException("Undefined type: " + typeId);
        };
    }
}
