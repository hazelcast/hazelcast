/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.AuthenticationFailureOp;
import com.hazelcast.internal.cluster.impl.operations.BeforeJoinCheckFailureOp;
import com.hazelcast.internal.cluster.impl.operations.ClusterMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.CommitClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.ConfigMismatchOp;
import com.hazelcast.internal.cluster.impl.operations.DemoteDataMemberOp;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.FetchMembersViewOp;
import com.hazelcast.internal.cluster.impl.operations.FinalizeJoinOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatComplaintOp;
import com.hazelcast.internal.cluster.impl.operations.HeartbeatOp;
import com.hazelcast.internal.cluster.impl.operations.JoinMastershipClaimOp;
import com.hazelcast.internal.cluster.impl.operations.JoinRequestOp;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.MasterResponseOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.cluster.impl.operations.MergeClustersOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.SplitBrainMergeValidationOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.cluster.impl.operations.WhoisMasterOp;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.util.function.Supplier;

public final class ClusterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 0;

    // never reassign numbers, in case of deletion leave a number gap.
    public static final int AUTH_FAILURE = 0;
    public static final int ADDRESS = 1;
    public static final int MEMBER = 2;
    public static final int HEARTBEAT = 3;
    public static final int CONFIG_CHECK = 4;
    public static final int MEMBER_HANDSHAKE = 5;
    public static final int MEMBER_INFO_UPDATE = 6;
    public static final int FINALIZE_JOIN = 7;
    public static final int BEFORE_JOIN_CHECK_FAILURE = 8;
    public static final int CHANGE_CLUSTER_STATE = 9;
    public static final int CONFIG_MISMATCH = 10;
    public static final int CLUSTER_MISMATCH = 11;
    public static final int SPLIT_BRAIN_MERGE_VALIDATION = 12;
    public static final int JOIN_REQUEST_OP = 13;
    public static final int LOCK_CLUSTER_STATE = 14;
    public static final int MASTER_CLAIM = 15;
    public static final int WHOIS_MASTER = 16;
    public static final int ENDPOINT_QUALIFIER = 17;
    public static final int MERGE_CLUSTERS = 18;
    public static final int POST_JOIN = 19;
    public static final int ROLLBACK_CLUSTER_STATE = 20;
    public static final int MASTER_RESPONSE = 21;
    public static final int SHUTDOWN_NODE = 22;
    public static final int TRIGGER_MEMBER_LIST_PUBLISH = 23;
    public static final int CLUSTER_STATE_TRANSACTION_LOG_RECORD = 24;
    public static final int MEMBER_INFO = 25;
    public static final int JOIN_MESSAGE = 26;
    public static final int JOIN_REQUEST = 27;
    public static final int MIGRATION_INFO = 28;
    public static final int MEMBER_VERSION = 29;
    public static final int CLUSTER_STATE_CHANGE = 30;
    public static final int SPLIT_BRAIN_JOIN_MESSAGE = 31;
    public static final int VERSION = 32;
    public static final int FETCH_MEMBER_LIST_STATE = 33;
    public static final int EXPLICIT_SUSPICION = 34;
    public static final int MEMBERS_VIEW = 35;
    public static final int TRIGGER_EXPLICIT_SUSPICION = 36;
    public static final int MEMBERS_VIEW_METADATA = 37;
    public static final int HEARTBEAT_COMPLAINT = 38;
    public static final int PROMOTE_LITE_MEMBER = 39;
    public static final int VECTOR_CLOCK = 40;
    public static final int DEMOTE_DATA_MEMBER = 41;
    public static final int MEMBERS_VIEW_RESPONSE = 42;

    static final int LEN = MEMBERS_VIEW_RESPONSE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[AUTH_FAILURE] = AuthenticationFailureOp::new;
        constructors[ADDRESS] = Address::new;
        constructors[MEMBER] = MemberImpl::new;
        constructors[HEARTBEAT] = HeartbeatOp::new;
        constructors[CONFIG_CHECK] = ConfigCheck::new;
        constructors[MEMBER_HANDSHAKE] = MemberHandshake::new;
        constructors[MEMBER_INFO_UPDATE] = MembersUpdateOp::new;
        constructors[FINALIZE_JOIN] = FinalizeJoinOp::new;
        constructors[BEFORE_JOIN_CHECK_FAILURE] = BeforeJoinCheckFailureOp::new;
        constructors[CHANGE_CLUSTER_STATE] = CommitClusterStateOp::new;
        constructors[CONFIG_MISMATCH] = ConfigMismatchOp::new;
        constructors[CLUSTER_MISMATCH] = ClusterMismatchOp::new;
        constructors[SPLIT_BRAIN_MERGE_VALIDATION] = SplitBrainMergeValidationOp::new;
        constructors[JOIN_REQUEST_OP] = JoinRequestOp::new;
        constructors[LOCK_CLUSTER_STATE] = LockClusterStateOp::new;
        constructors[MASTER_CLAIM] = JoinMastershipClaimOp::new;
        constructors[WHOIS_MASTER] = WhoisMasterOp::new;
        constructors[MERGE_CLUSTERS] = MergeClustersOp::new;
        constructors[POST_JOIN] = OnJoinOp::new;
        constructors[ROLLBACK_CLUSTER_STATE] = RollbackClusterStateOp::new;
        constructors[MASTER_RESPONSE] = MasterResponseOp::new;
        constructors[SHUTDOWN_NODE] = ShutdownNodeOp::new;
        constructors[TRIGGER_MEMBER_LIST_PUBLISH] = TriggerMemberListPublishOp::new;
        constructors[CLUSTER_STATE_TRANSACTION_LOG_RECORD] = ClusterStateTransactionLogRecord::new;
        constructors[MEMBER_INFO] = MemberInfo::new;
        constructors[JOIN_MESSAGE] = JoinMessage::new;
        constructors[JOIN_REQUEST] = JoinRequest::new;
        constructors[MIGRATION_INFO] = MigrationInfo::new;
        constructors[MEMBER_VERSION] = MemberVersion::new;
        constructors[CLUSTER_STATE_CHANGE] = ClusterStateChange::new;
        constructors[SPLIT_BRAIN_JOIN_MESSAGE] = SplitBrainJoinMessage::new;
        constructors[VERSION] = Version::new;
        constructors[FETCH_MEMBER_LIST_STATE] = FetchMembersViewOp::new;
        constructors[EXPLICIT_SUSPICION] = ExplicitSuspicionOp::new;
        constructors[MEMBERS_VIEW] = MembersView::new;
        constructors[TRIGGER_EXPLICIT_SUSPICION] = TriggerExplicitSuspicionOp::new;
        constructors[MEMBERS_VIEW_METADATA] = MembersViewMetadata::new;
        constructors[HEARTBEAT_COMPLAINT] = HeartbeatComplaintOp::new;
        constructors[PROMOTE_LITE_MEMBER] = PromoteLiteMemberOp::new;
        constructors[DEMOTE_DATA_MEMBER] = DemoteDataMemberOp::new;
        constructors[MEMBERS_VIEW_RESPONSE] = MembersViewResponse::new;
        constructors[VECTOR_CLOCK] = VectorClock::new;
        constructors[ENDPOINT_QUALIFIER] = EndpointQualifier::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
